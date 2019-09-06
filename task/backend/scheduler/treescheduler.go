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
	active    bool
	wg        sync.WaitGroup

	sm *SchedulerMetrics
}

func (s *TreeScheduler) Tree() *btree.BTree {
	return s.scheduled
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
		nextTime:  map[ID]int64{},
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
	s.wg.Add(1)
	log.Println("q1")
	go func() {
		defer s.wg.Done()
		log.Println("q2")
		//s.scheduled.Descend(s.descendIterator(time.Time{}))
	schedulerLoop:
		for {
			s.Lock()
			min := s.scheduled.Min()
			if min == nil {
				if !s.timer.Stop() {
					<-s.timer.C()
				}
				s.when = s.time.Now().Add(maxWaitTime)
				s.timer.Reset(maxWaitTime)
			}
			//for i := 0; i < 10; i++ {
			//	time.Sleep(100 * time.Millisecond)
			//	//log.Println("before select", s.when, s.time.Now(), min.(Item).next, s.timer.(*MockTimer).fireTime.Unix(), s.timer.(*MockTimer).Reset(0))
			//}
			s.Unlock()
			select {
			case <-s.done:
				s.Lock()
				log.Println("closing tree")
				s.timer.Stop()
				close(s.sema)
				s.Unlock()
				return
			case ts := <-s.timer.C():
				s.Lock()
				log.Println("timer fired")

				min := s.scheduled.Min() // grab a new item, because there could be a different item at the top of the queue
				if min == nil {
					if !s.timer.Stop() {
						<-s.timer.C()
					}
					log.Println("setting timer to maxtime")
					s.when = s.time.Now().Add(maxWaitTime)
					s.timer.Reset(maxWaitTime)
					s.Unlock()
					continue schedulerLoop
				}
				minItem := min.(Item) // we want it to panic if things other than Items are populating the scheduler, as it is something we can't recover from.
				s.timer.Reset(s.time.Until(time.Unix(minItem.next, 0)))
				s.Unlock()
				for s.process(ts) {
					select {
					case <-s.done:
						close(s.sema)
						return
					}
				}
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

// process processes the next item in the tree if it is before on on time ts.
func (s *TreeScheduler) process(ts time.Time) bool {
	log.Println("processing")
	s.Lock()
	min := s.scheduled.DeleteMin() // so other threads won't grab it
	s.Unlock()
	if min == nil {
		log.Println("processing min==nil")

		return false
	}
	it := min.(Item) // we want it to panic if things other than Items are populating the scheduler, as it is something we can't recover from.
	defer func() {
		s.Lock()
		s.scheduled.ReplaceOrInsert(it)
		s.nextTime[it.ID] = it.next
		s.Unlock()
	}() // cleanup

	if time.Unix(it.next+it.Offset, 0).After(ts) {
		log.Println("after", time.Unix(it.next+it.Offset, 0), ts)
		return false
	}
	log.Println("survived")
	// we then do a sem wait, but because if we are waiting too long on the semafor our data might be stale, we need a timeout on this where we go back and grab the next item again, if it is.
	{
		semWaitTimeout := time.NewTimer(10 * time.Millisecond)
		select {
		case s.sema <- struct{}{}:
			semWaitTimeout.Stop()
		case <-semWaitTimeout.C:
			log.Println("sem wait timeout exceeded")
			return true // because we want to keep trying if we are in this case
		}
	}
	log.Println("survived 2")

	s.sm.startExecution(it.ID, time.Since(time.Unix(it.next, 0)))
	prom, err := s.executor(context.Background(), it.ID, time.Unix(it.next, 0))
	if err != nil {
		s.onErr(context.Background(), it.ID, 0, time.Unix(it.next, 0), err)
	}
	log.Println("survived 3")

	//t, err := it.cron.Next(s.time.Unix(it.next, 0))
	//// if the call to Next fails, then we really can't get a timestamp for the error
	//if err != nil {
	//	it.nonce++
	//	s.onErr(context.TODO(), it.ID, prom.ID(), ts, err)
	//}
	//
	//it.next = t.Unix()
	if err := it.updateNext(); err != nil {
		s.onErr(context.TODO(), it.ID, prom.ID(), ts, err)
	}
	// we need to return the Item to the scheduled before calling s.onErr
	if prom == nil {
		return true
	}
	s.Lock()
	s.running.ReplaceOrInsert(runningItem{cancel: prom.Cancel, runID: prom.ID(), taskID: ID(it.ID)})
	s.Unlock()

	go func(it Item, prom Promise) {
		fmt.Println("here 4")
		defer func() {
			s.wg.Done()
			<-s.sema
		}()
		<-prom.Done()
		err := prom.Error()
		if err != nil {
			s.onErr(context.Background(), it.ID, prom.ID(), time.Unix(it.next, 0), err)
			return
		}
		fmt.Println("here 0 3")
		s.Lock()
		s.running.Delete(runningItem{cancel: prom.Cancel, runID: ID(prom.ID()), taskID: ID(it.ID)})
		s.Unlock()

		s.sm.finishExecution(it.ID, prom.Error() == nil, backend.RunStarted, time.Since(time.Unix(it.next, 0)))
		fmt.Println("here 0 4")

		if err = prom.Error(); err != nil {
			s.onErr(context.Background(), it.ID, 0, time.Unix(it.next, 0), err)
			return
		}
		fmt.Println("here 0 5")
	}(it, prom)
	return true
}

func (s *TreeScheduler) Now() time.Time {
	s.RLock()
	now := s.time.Now()
	s.RUnlock()
	return now
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
	s.scheduled.Delete(Item{
		next: nextTime,
		ID:   taskID,
	})
	delete(s.nextTime, taskID)

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
	it := Item{
		cron:   crSch,
		next:   nt.Unix(),
		ID:     id,
		Offset: int64(offset.Seconds()),
		last:   since.Unix(),
	}

	fmt.Println("thing done 3")

	s.Lock()
	defer func() {
		s.Unlock()
		log.Println("exiting Schedule", it.ID)
	}()
	nt = nt.Add(offset)
	if s.when.After(nt) {
		fmt.Println("thing done")
		s.when = nt
		if !s.timer.Stop() {
			fmt.Println("thing done q1")
			<-s.timer.C()
		}
		until := s.time.Until(s.when)
		if until < 0 {
			s.timer.Reset(0)
		} else {
			s.timer.Reset(s.time.Until(s.when))
		}
		fmt.Println("thing done q")

	}
	nextTime, ok := s.nextTime[id]
	if !ok {
		fmt.Println("thing done 4")
		s.nextTime[id] = it.next + it.Offset
		s.scheduled.ReplaceOrInsert(it)
		return nil
	}
	fmt.Println("thing done 2")

	// delete the old task run time
	s.scheduled.Delete(Item{
		next: nextTime,
		ID:   id,
	})

	// insert the new task run time
	s.scheduled.ReplaceOrInsert(it)
	return nil
}

func (s *TreeScheduler) Runs(taskID ID, limit int) []ID {
	s.RLock()
	defer func() {
		s.RUnlock()
		fmt.Println("saaaaaaaa")
	}()
	fmt.Println("bbbbbbbbb")

	iter, acc := s.runs(taskID, limit)
	fmt.Println("saaaaaaaa")
	s.running.AscendGreaterOrEqual(runningItem{taskID: 0}, iter)
	return acc
}

var maxItem = Item{
	next:  math.MaxInt64,
	nonce: int(^uint(0) >> 1),
	ID:    maxID,
}

// Item is a task in the scheduler.
type Item struct {
	cron   cron.Parsed
	next   int64
	Offset int64
	last   int64
	nonce  int // for retries
	ID     ID
}

// Less tells us if one Item is less than another
func (it Item) Less(bItem btree.Item) bool {
	it2 := bItem.(Item)
	return it.next < it2.next || (it.next == it2.next && (it.nonce < it2.nonce || it.nonce == it2.nonce && it.ID < it2.ID))
}

func (it *Item) updateNext() error {
	newNext, err := it.cron.Next(time.Unix(it.next, 0))
	if err != nil {
		return err
	}
	it.last = it.next
	it.next = newNext.Unix()
	return nil
}
