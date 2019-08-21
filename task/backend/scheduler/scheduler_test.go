package scheduler_test

import (
	"context"
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
	mockTime := scheduler.NewMockTime(now)
	sch, _, err := scheduler.NewScheduler(exe.Execute, scheduler.WithTime(mockTime))
	if err != nil {
		t.Fatal(err)
	}
	defer sch.Stop()
	err = sch.Schedule(1, "* * * * * * *", 10*time.Second, now.Add(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	mockTime.Set(now.Add(1000 * time.Second))
	timeOut := time.After(5 * time.Second)
	// a timeout for the test
	for i := 0; i < 10; i++ {
		select {
		case ts := <-c:
			if i == 0 && ts != now.Add(20*time.Second) {
				t.Errorf("Returned incorrect starting time")
			}
			if !ts.Equal(now.Add(time.Duration(i) * time.Second)) {
				t.Errorf("Returned incorrect time for run #%d", i)
			}
			t.Log("kek")
		case <-timeOut:
			t.Errorf("test timed out early after %d", i+1)
			return
		}
	}
}
