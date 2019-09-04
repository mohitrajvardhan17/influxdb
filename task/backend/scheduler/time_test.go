package scheduler

import (
	"fmt"
	"testing"
	"time"
)

func TestStdTime_Now(t *testing.T) {
	t1 := stdTime{}.Now()
	time.Sleep(time.Nanosecond)
	t2 := stdTime{}.Now()
	if !t1.Before(t2) {
		t.Fatal()
	}
}

func TestStdTime_Unix(t *testing.T) {
	now := time.Now()
	t1 := stdTime{}.Unix(now.Unix(), int64(now.Nanosecond()))
	if !t1.Equal(now) {
		t.Fatal("expected the two times to be equivalent but they were not")
	}
}

func TestMockTimer(t *testing.T) {
	timeForComparison := time.Date(2016, 2, 3, 4, 5, 6, 7, time.UTC)
	mt := NewMockTime(timeForComparison)
	timer := mt.NewTimer(10 * time.Second)
	select {
	case <-timer.C():
		t.Fatalf("expected timer not to fire till time was up, but did")
	default:
	}
	go mt.Set(timeForComparison.Add(10 * time.Second))
	select {
	case <-timer.C():
	case <-time.After(3 * time.Second):
		t.Fatal("expected timer to fire when time was up, but it didn't, it fired after a 3 second timeout")
	}
	timer.Reset(33 * time.Second)
	go mt.Set(timeForComparison.Add(50 * time.Second))
	select {
	case <-timer.C():
	case <-time.After(4 * time.Second):
		t.Fatal("expected timer to fire when time was up, but it didn't, it fired after a 4 second timeout")
	}
	if !timer.Stop() {
		<-timer.C()
	}
	timer.Reset(10000 * time.Second)
	select {
	case ts := <-timer.C():
		t.Errorf("expected timer to NOT fire if time was not up, but it did at ts: %s", ts)
	default:
	}

	timer2 := mt.NewTimer(10000 * time.Second)
	select {
	case ts := <-timer2.C():
		t.Errorf("expected timer to NOT fire if time was not up, but it did at ts: %s", ts)
	case <-time.After(time.Second):
	default:
	}

}

func TestMockTimer_Stop(t *testing.T) {
	timeForComparison := time.Date(2016, 2, 3, 4, 5, 6, 7, time.UTC)
	mt := NewMockTime(timeForComparison)
	timer := mt.NewTimer(10 * time.Second)
	fmt.Println("here")
	if !timer.Stop() {
		t.Fatal("expected MockTimer.Stop() to be true  if it hadn't fired yet")
	}
	fmt.Println("here 1")

	if !timer.Stop() {
		select {
		case <-timer.C():
		case <-time.After(2 * time.Second):
			t.Fatal("timer didn't fire to clear when it should have")
		}
	} else {
		t.Fatalf("Expected MockTimer.Stop() to be false when it was already stopped but it wasn't")
	}
	fmt.Println("here 2")

	timer.Reset(time.Second)
	go mt.Set(timeForComparison.Add(20 * time.Second))
	select {
	case <-timer.C():
	case <-time.After(2 * time.Second):
		t.Fatal("timer didn't fire when it should have")
	}
	fmt.Println("here 3")
	if !timer.Stop() {
		select {
		case <-timer.C():
		case <-time.After(2 * time.Second):
			t.Fatal("timer didn't fire to clear when it should have")
		}
	} else {
		t.Fatalf("Expected MockTimer.Stop() to be false when it was already fired but it wasn't")
	}
}
