package scheduler_test

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/task/backend/scheduler"
)

type mockExecutor struct {
	sync.Mutex
	fn  func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time)
	Err error
}

func (e *mockExecutor) Execute(ctx context.Context, id scheduler.ID, scheduledAt time.Time) (scheduler.Promise, error) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{}, 1)
	select {
	case <-ctx.Done():
	default:
		go func() {
			e.fn(&sync.Mutex{}, ctx, id, scheduledAt)
			done <- struct{}{}
		}()

	}

	return &MockPromise{
		id:     scheduler.ID(rand.Int63()),
		cancel: cancel,
		done:   done,
		err:    e.Err,
	}, nil
}

type MockPromise struct {
	id     scheduler.ID
	cancel context.CancelFunc
	done   <-chan struct{}
	err    error
}

func (p *MockPromise) ID() scheduler.ID {
	return p.id
}

// Cancel a promise, identical to calling executor.Cancel().
func (p *MockPromise) Cancel(_ context.Context) {
	p.cancel()
}

// Done returns a read only channel that when closed indicates the execution is complete.
func (p *MockPromise) Done() <-chan struct{} {
	return p.done
}

// Error returns an error only when the execution is complete.
// This is a hanging call until Done() is closed.
func (p *MockPromise) Error() error {
	return p.err
}

func TestSchedule_Next(t *testing.T) {
	now := time.Now()
	c := make(chan time.Time, 10)
	exe := mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time) {
		l.Lock()
		defer l.Unlock()
		log.Println("executing the executor")
		select {
		case <-ctx.Done():
			t.Log("ctx done")
		case c <- scheduledAt:
			t.Log("adding to queue")
		default:
			t.Errorf("called the executor too many times")
		}
	}}
	log.Println("here now 2")
	mockTime := scheduler.NewMockTime(now)
	sch, _, err := scheduler.NewScheduler(exe.Execute, scheduler.WithTime(mockTime))
	if err != nil {
		t.Fatal(err)
	}
	//defer sch.Stop()
	err = sch.Schedule(1, "* * * * * * *", time.Second, now.Add(-20*time.Second))
	log.Println("6666666")
	if err != nil {
		t.Fatal(err)
	}
	it := sch.Tree().Clone().Min().(scheduler.Item)
	log.Println("777777777")

	t.Log(sch.Runs(1, 90), it.ID, it.Offset)
	log.Println("8888888")
	t.Log("here now 3", mockTime.Get())
	log.Println("55555555")

	go func() {
		time.Sleep(time.Millisecond)
		mockTime.Set(now.Add(4000 * time.Second))
		fmt.Println("here now 5")
	}()
	fmt.Println("here now 4", mockTime.Get())
	select {
	case <-c:
		t.Log("**************************** woot ************************")
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out", sch.Now().Unix(), sch.When().Unix())
	}
	fmt.Println("99999999")
}

func TestTreeScheduler_Stop(t *testing.T) {
	now := time.Now().Add(-20 * time.Second)
	mockTime := scheduler.NewMockTime(now)
	exe := mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time) {}}
	sch, _, err := scheduler.NewScheduler(exe.Execute, scheduler.WithTime(mockTime))
	if err != nil {
		t.Fatal(err)
	}
	sch.Stop()
}
