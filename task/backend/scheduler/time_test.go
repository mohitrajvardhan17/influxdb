package scheduler

import (
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
	case <-time.After(10 * time.Second):
		t.Error("expected timer to fire when time was up, but it didn't, it fired after a 10 second timeout")
	}
	//if !timer.Stop() {
	//	fmt.Println("here1")
	//	<-timer.C()
	//	fmt.Println("here2")
	//}
	//timer.Reset(33 * time.Second)
	//go mt.Set(timeForComparison.Add(50 * time.Second))
	//fmt.Println("here")
	//fmt.Println("here0")
	//
	//select {
	//case <-timer.C():
	//case <-time.After(10 * time.Second):
	//	t.Error("expected timer to fire when time was up, but it didn't, it fired after a 10 second timeout")
	//}
}

func TestMockTimer_Stop(t *testing.T) {
	timeForComparison := time.Date(2016, 2, 3, 4, 5, 6, 7, time.UTC)
	mt := NewMockTime(timeForComparison)
	timer := mt.NewTimer(10 * time.Second)
	if !timer.Stop() {
		t.Fatal("expected MockTimer.Stop() to be true  if it hadn't fired yet")
	}
	if timer.Stop() {
		t.Fatalf("Expected MockTimer.Stop() to be false when it was already stopped but it wasn't")
	}
	timer.Reset(10 * time.Second)
	mt.Set(timeForComparison.Add(10 * time.Second))
	if timer.Stop() {
		t.Fatalf("Expected MockTimer.Stop() to be false when it was already fired but it wasn't")
	}
}

func TestTimer(t *testing.T) {
	timer := time.NewTimer(30 * time.Second)
	timer.Stop()

}
