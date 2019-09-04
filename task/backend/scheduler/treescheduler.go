package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/influxdata/influxdb/task/backend"

	"github.com/google/btree"
	"github.com/influxdata/cron"
)

const (
	cancelTimeOut             = 30 * time.Second
	maxWaitTime               = time.Hour
	degreeBtreeScheduled      = 3
	degreeBtreeRunning        = 3
	defaultMaxRunsOutstanding = 1 << 16
)

type runningItem struct {
	cancel func(ctx context.Context)
	runID  ID
	taskID ID
}

func (it runningItem) Less(bItem btree.Item) bool {
	it2 := bItem.(runningItem)
	return it.taskID < it2.taskID || (it.taskID == it2.taskID && it.runID < it2.runID)
}

// TreeScheduler is a Scheduler based on a btree
type TreeScheduler struct {
	sync.RWMutex
	scheduled *btree.BTree
	running   *btree.BTree
	nextTime  map[ID]int64 // we need this index so we can delete items from the scheduled
	when      time.Time
	executor  func(ctx context.Context, id ID, scheduledAt time.Time) (Promise, error)
	onErr     func(ctx context.Context, taskID ID, runID ID, scheduledAt time.Time, err error) bool
	time      Time
	timer     Timer
	done      chan struct{}
	sema      chan struct{}
	wg        sync.WaitGroup

	sm *SchedulerMetrics
}

// clearTask is a method for deleting a range of tasks.
// TODO(docmerlin): add an actual ranged delete to github.com/google/btree
func (s *TreeScheduler) clearTask(taskID ID) btree.ItemIterator {
	return func(i btree.Item) bool {
		del := i.(runningItem).taskID == taskID
		if !del {
			return false
		}
		s.running.Delete(runningItem{taskID: taskID})
		return true
	}
}

// runs is a method for accumulating the running runs of a task.
func (s *TreeScheduler) runs(taskID ID, limit int) (btree.ItemIterator, []ID) {
	acc := make([]ID, 0, limit)
	return func(i btree.Item) bool {
		ritem := i.(runningItem)
		match := ritem.taskID == taskID
		if !match {
			return false
		}
		acc = append(acc, ritem.runID)
		return true
	}, acc
}

type ExecutorFunc func(ctx context.Context, id ID, scheduledAt time.Time) (Promise, error)

type ErrorFunc func(ctx context.Context, taskID ID, runID ID, scheduledAt time.Time, err error) bool

type treeSchedulerOptFunc func(t *TreeScheduler) error

func WithOnErrorFn(fn ErrorFunc) treeSchedulerOptFunc {
	return func(t *TreeScheduler) error {
		t.onErr = fn
		return nil
	}
}

func WithMaxRunsOutsanding(n int) treeSchedulerOptFunc {
	return func(t *TreeScheduler) error {
		t.sema = make(chan struct{}, n)
		return nil
	}
}

func WithTime(t Time) treeSchedulerOptFunc {
	return func(sch *TreeScheduler) error {
		sch.time = t
		return nil
	}
}

// Executor is any function that accepts an ID, a time, and a duration.
// OnErr is a function that takes am error, it is called when we cannot find a viable time before jan 1, 2100.  The default behavior is to drop the task on error.
func NewScheduler(Executor ExecutorFunc, opts ...treeSchedulerOptFunc) (*TreeScheduler, *SchedulerMetrics, error) {
	s := &TreeScheduler{
		executor:  Executor,
		scheduled: btree.New(degreeBtreeScheduled),
		running:   btree.New(degreeBtreeRunning),
		onErr:     func(_ context.Context, _ ID, _ ID, _ time.Time, _ error) bool { return true },
		sema:      make(chan struct{}, defaultMaxRunsOutstanding),
		time:      stdTime{},
		done:      make(chan struct{}, 1),
	}

	// apply options
	for i := range opts {
		if err := opts[i](s); err != nil {
			return nil, nil, err
		}
	}

	s.sm = NewSchedulerMetrics(s)
	s.when = s.time.Now().Add(maxWaitTime)
	s.timer = s.time.NewTimer(maxWaitTime)
	if Executor == nil {
		return nil, nil, errors.New("Executor must be a non-nil function")
	}

	log.Println("stopping! q1")

	go func() {
		log.Println("stopping! q2")
	mainSchedLoop:
		for {
			fmt.Println("here 0", s.when)
			select {
			case <-s.done:
				fmt.Println("here 12")
				s.Lock()
				s.timer.Stop()
				fmt.Println("here 11")
				s.Unlock()
				close(s.sema)
				fmt.Println("here 1")
				return
			case ts := <-s.timer.C():
				//fmt.Println("here 2 fired")
				//iti := s.scheduled.DeleteMin()
				//fmt.Println("here 3")
				//if iti == nil {
				//	s.Lock()
				//	//if !s.timer.Stop() {
				//	//	<-s.timer.C()
				//	//}
				//	s.timer.Reset(maxWaitTime)
				//	s.Unlock()
				//	continue mainSchedLoop
				//}
				s.scheduled.Descend(s.descendIterator(ts))
			}
		}
	}()
	return s, s.sm, nil
}

func (s *TreeScheduler) Stop() {
	s.RLock()
	semaCap := cap(s.sema)
	s.RUnlock()
	select {
	case s.done <- struct{}{}:
	default:
	}
	// this is to make sure the semaphore is closed.  It tries to pull cap+1 empty structs from the semaphore, only possible when closed
	for i := 0; i <= semaCap; i++ {
		<-s.sema
	}
	s.wg.Wait()
}

// descendIterator is the btree.ItemIterator that actually calls the executor.
func (s *TreeScheduler) descendIterator(ts time.Time) btree.ItemIterator {
	t := ts.Unix()
	return func(iti btree.Item) bool {
		it := iti.(item) // its unrecoverable if somehow non-items got into the tree, so we panic here.
		if it.next > t {
			return false
		}
		s.sm.startExecution(it.id, time.Since(time.Unix(it.next, 0)))
		prom, err := s.executor(context.Background(), it.id, time.Unix(it.next, 0))
		if err != nil {
			s.onErr(context.Background(), it.id, 0, time.Unix(it.next, 0), err)
		}
		t, err := it.cron.Next(s.time.Unix(it.next, 0))
		it.next = t.Unix()
		// we need to return the item to the scheduled before calling s.onErr
		if err != nil {
			it.nonce++
			s.onErr(context.TODO(), it.id, prom.ID(), time.Unix(it.next, 0), err)
		}
		s.scheduled.ReplaceOrInsert(it)
		if prom == nil {
			return true
		}
		s.Lock()
		s.running.ReplaceOrInsert(runningItem{cancel: prom.Cancel, runID: prom.ID(), taskID: ID(it.id)})
		s.Unlock()

		s.sema <- struct{}{}
		go func(it item, prom Promise) {
			fmt.Println("here 4")
			defer func() {
				s.wg.Done()
				<-s.sema
			}()
			<-prom.Done()
			err := prom.Error()
			if err != nil {
				s.onErr(context.Background(), it.id, prom.ID(), time.Unix(it.next, 0), err)
				return
			}
			fmt.Println("here 0 3")
			s.Lock()
			s.running.Delete(runningItem{cancel: prom.Cancel, runID: ID(prom.ID()), taskID: ID(it.id)})
			s.Unlock()

			s.sm.finishExecution(it.id, prom.Error() == nil, backend.RunStarted, time.Since(time.Unix(it.next, 0)))
			fmt.Println("here 0 4")

			if err = prom.Error(); err != nil {
				s.onErr(context.Background(), it.id, 0, time.Unix(it.next, 0), err)
				return
			}
			fmt.Println("here 0 5")
		}(it, prom)
		return true
	}
}

// When gives us the next time the scheduler will run a task.
func (s *TreeScheduler) When() time.Time {
	s.RLock()
	w := s.when
	s.RUnlock()
	return w
}

// Release releases a task, if it doesn't own the task it just returns.
// Release also cancels the running task.
// Task deletion would be faster if the tree supported deleting ranges.
func (s *TreeScheduler) Release(taskID ID) error {
	s.sm.release(taskID)
	s.Lock()
	defer s.Unlock()
	nextTime, ok := s.nextTime[taskID]
	if !ok {
		return nil
	}

	// delete the old task run time
	s.scheduled.Delete(item{
		next: nextTime,
		id:   taskID,
	})

	s.running.AscendGreaterOrEqual(runningItem{taskID: taskID}, s.clearTask(taskID))
	return nil
}

// put puts an Item on the TreeScheduler.
func (s *TreeScheduler) Schedule(id ID, cronString string, offset time.Duration, since time.Time) error {
	s.sm.schedule(id)
	crSch, err := cron.ParseUTC(cronString)
	if err != nil {
		return err
	}
	nt, err := crSch.Next(since)
	if err != nil {
		return err
	}
	it := item{
		cron: crSch,
		next: nt.Add(offset).Unix(),
		id:   id,
	}
	fmt.Println("thing done 3")

	s.Lock()
	defer s.Unlock()

	if s.when.After(nt) {
		fmt.Println("thing done")
		s.when = nt
		if !s.timer.Stop() {
			fmt.Println("thing done q1")
			<-s.timer.C()
		}
		fmt.Println("thing done q")

		s.timer.Reset(time.Until(s.when))
	}
	nextTime, ok := s.nextTime[id]
	if !ok {
		fmt.Println("thing done 4")
		s.scheduled.ReplaceOrInsert(it)
		return nil
	}
	fmt.Println("thing done 2")

	// delete the old task run time
	s.scheduled.Delete(item{
		next: nextTime,
		id:   id,
	})

	// insert the new task run time
	s.scheduled.ReplaceOrInsert(it)
	return nil
}

func (s *TreeScheduler) Runs(taskID ID, limit int) []ID {
	s.RLock()
	defer s.RUnlock()
	iter, acc := s.runs(taskID, limit)
	s.running.AscendGreaterOrEqual(runningItem{taskID: 0}, iter)
	return acc
}

var maxItem = item{
	next:  math.MaxInt64,
	nonce: int(^uint(0) >> 1),
	id:    maxID,
}

// Item is a task in the scheduler.
type item struct {
	cron   cron.Parsed
	next   int64
	last   int64
	nonce  int // for retries
	offset int
	id     ID
}

// Less tells us if one Item is less than another
func (it item) Less(bItem btree.Item) bool {
	it2 := bItem.(item)
	return it.next < it2.next || (it.next == it2.next && (it.nonce < it2.nonce || it.nonce == it2.nonce && it.id < it2.id))
}

func (it *item) updateNext() error {
	newNext, err := it.cron.Next(time.Unix(it.last, 0))
	if err != nil {
		return err
	}
	it.last = it.next
	it.next = newNext.Unix()
	return nil
}
