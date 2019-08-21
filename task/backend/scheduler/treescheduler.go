package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/influxdata/influxdb/task/backend"

	"github.com/google/btree"
	"github.com/influxdata/cron"
)

const (
	cancelTimeOut = 30 * time.Second

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

const maxWaitTime = 1000000 * time.Hour

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
	}

	// apply options
	for i := range opts {
		if err := opts[i](s); err != nil {
			return nil, nil, err
		}
	}

	s.sm = NewSchedulerMetrics(s)
	s.when = s.time.Now().Add(maxWaitTime)
	s.timer = s.time.NewTimer(s.time.Until(s.when)) //time.Until(s.when))
	if Executor == nil {
		return nil, nil, errors.New("Executor must be a non-nil function")
	}
	go func() {
		for {
			fmt.Println("here 0")
			select {
			case <-s.done:
				s.Lock()
				s.timer.Stop()
				s.Unlock()
				close(s.sema)
				fmt.Println("here 1")
				return
			case <-s.timer.C():
				fmt.Println("here 2")
				iti := s.scheduled.DeleteMin()
				fmt.Println("here 3")
				if iti == nil {
					s.Lock()
					//if !s.timer.Stop() {
					//	<-s.timer.C()
					//}
					s.timer.Reset(maxWaitTime)
					s.Unlock()
					continue
				}

				it := iti.(item)
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
					break
				}
				s.Lock()
				s.running.ReplaceOrInsert(runningItem{cancel: prom.Cancel, runID: prom.ID(), taskID: ID(it.id)})
				s.Unlock()

				s.wg.Add(1)
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
					s.Lock()
					s.running.Delete(runningItem{cancel: prom.Cancel, runID: ID(prom.ID()), taskID: ID(it.id)})
					s.Unlock()

					s.sm.finishExecution(it.id, prom.Error() == nil, backend.RunStarted, time.Since(time.Unix(it.next, 0)))

					if err = prom.Error(); err != nil {
						s.onErr(context.Background(), it.id, 0, time.Unix(it.next, 0), err)
						return
					}
				}(it, prom)
			}
		}
	}()
	return s, s.sm, nil
}

func (s *TreeScheduler) Stop() {
	s.RLock()
	semaCap := cap(s.sema)
	s.RUnlock()
	s.done <- struct{}{}

	// this is to make sure the semaphore is closed.  It tries to pull cap+1 empty structs from the semaphore, only possible when closed
	for i := 0; i <= semaCap; i++ {
		<-s.sema
	}
	s.wg.Wait()
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
			<-s.timer.C()
		}
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

// Item is a task in the scheduler.
type item struct {
	cron   cron.Parsed
	next   int64
	nonce  int // for retries
	offset int
	id     ID
}

// Less tells us if one Item is less than another
func (it item) Less(bItem btree.Item) bool {
	it2 := bItem.(item)
	return it.next < it2.next || (it.next == it2.next && (it.nonce < it2.nonce || it.nonce == it2.nonce && it.id < it2.id))
}
