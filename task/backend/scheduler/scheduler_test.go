package scheduler_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb/task/backend/scheduler"
)

type mockExecutor struct {
	sync.Mutex
	fn func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time)
}

func (e *mockExecutor) Execute(ctx context.Context, id scheduler.ID, scheduledAt time.Time) (scheduler.Promise, error) {
	select {
	case <-ctx.Done():
	}
	return nil, nil
}

func TestSchedule_Next(t *testing.T) {
	now := time.Now().Add(-20 * time.Second)
	c := make(chan time.Time, 10)
	exe := mockExecutor{fn: func(l *sync.Mutex, ctx context.Context, id scheduler.ID, scheduledAt time.Time) {
		l.Lock()
		select {
		case <-ctx.Done():
		case c <- scheduledAt:
		default:
			t.Errorf("called the executor too many times")
		}
		defer l.Unlock()
	}}
	log.Println("here now 2")
	mockTime := scheduler.NewMockTime(now)
	sch, _, err := scheduler.NewScheduler(exe.Execute, scheduler.WithTime(mockTime))
	if err != nil {
		t.Fatal(err)
	}
	//defer sch.Stop()
	err = sch.Schedule(1, "* * * * * * *", 10*time.Second, now.Add(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	log.Println("here now 3", mockTime.Get())

	mockTime.Set(now.Add(1000 * time.Second))
	fmt.Println("here now 44 ")

	for i := 0; i < 10; i++ {
		fmt.Println("here now 4 ", i)

		select {
		case ts := <-c:
			fmt.Println("here now 7 ", i)

			if i == 0 && ts != now.Add(20*time.Second) {
				t.Fatal("Returned incorrect starting time")
			}
			if !ts.Equal(now.Add(time.Duration(i) * time.Second)) {
				t.Fatalf("Returned incorrect time for run #%d", i)
			}
			log.Println("here now 5 ", i)
		case <-time.After(3 * time.Second): // a timeout for the test
			t.Fatalf("test timed out early after %d iterations", i+1)
			return
		}
		fmt.Println("here now 6 ", i)
	}
	log.Println("*********************8here now 1")
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
