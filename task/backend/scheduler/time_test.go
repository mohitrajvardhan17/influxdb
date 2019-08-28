package scheduler

import (
	"log"
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
	log.Println("here 1")
	select {
	case <-timer.C():
		t.Fatalf("expected timer not to fire till time was up, but did")
	default:
		log.Println("here 3")
	}
	go mt.Set(timeForComparison.Add(10 * time.Second))
	select {
	case <-timer.C():
		log.Println("here 2")
	case <-time.After(3 * time.Second):
		t.Fatal("expected timer to fire when time was up, but it didn't, it fired after a 3 second timeout")
	}
	timer.Reset(33 * time.Second)
	log.Println("here 4")
	go mt.Set(timeForComparison.Add(50 * time.Second))
	log.Println("here 5")
	select {
	case <-timer.C():
		log.Println("here 5")
	case <-time.After(4 * time.Second):
		t.Fatal("expected timer to fire when time was up, but it didn't, it fired after a 4 second timeout")
	}
	log.Println("here 6")
	timer.Reset(10000 * time.Second)
	select {
	case <-timer.C():
		t.Error("expected timer to NOT fire if time was not up, but it did")
	default:
	}

	timer2 := mt.NewTimer(10000 * time.Second)
	select {
	case <-timer2.C():
		t.Error("expected timer to NOT fire if time was not up, but it did")
	case <-time.After(time.Second):
	default:
	}

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
	timer.Reset(12 * time.Second)
	mt.Set(timeForComparison.Add(20 * time.Second))
	if timer.Stop() {
		t.Fatalf("Expected MockTimer.Stop() to be false when it was already fired but it wasn't")
	}
}

func TestTimer(t *testing.T) {
	timer := time.NewTimer(1 * time.Second)
	if !timer.Stop() {
		<-timer.C
	}
	timer.Reset(10 * time.Second)
	if !timer.Stop() {
		<-timer.C
	}
}
