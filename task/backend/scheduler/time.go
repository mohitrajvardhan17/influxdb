package scheduler

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Time is an interface to allow us to mock time.
type Time interface {
	Now() time.Time
	Unix(seconds, nanoseconds int64) time.Time
	NewTimer(d time.Duration) Timer
	Until(time.Time) time.Duration
}

type stdTime struct{}

// Now gives us the current time as time.Time would
func (stdTime) Now() time.Time {
	return time.Now()
}

// Unix gives us the time given seconds and nanoseconds.
func (stdTime) Unix(sec, nsec int64) time.Time {
	return time.Unix(sec, nsec)
}

func (stdTime) Until(t time.Time) time.Duration {
	return time.Until(t)
}

// NewTimer gives us a Timer that fires after duration d.
func (stdTime) NewTimer(d time.Duration) Timer {
	t := time.NewTimer(d)
	return &stdTimer{*t}
}

// Timer is an interface to allow us to mock out timers.  It has behavior like time.Timer
type Timer interface {
	C() <-chan time.Time
	Reset(d time.Duration) bool
	Stop() bool
}

// stdTimer is a Timer that wraps time.Time.
type stdTimer struct {
	time.Timer
}

// C returns a <-chan time.Time  and can be used much like time.Timer.C.
func (t *stdTimer) C() <-chan time.Time {
	return t.Timer.C
}

// MockTime is a time that mocks out some methods of time.Time.
// It doesn't advance the time over time, but only changes it with calls to Set.
// Use NewMockTime to create Mocktimes, don't instanciate the struct directly unless you want to mess with the sync Cond.
type MockTime struct {
	sync.RWMutex
	*sync.Cond
	T time.Time
}

// NewMockTime create a mock of time that returns the underlying time.Time.
func NewMockTime(t time.Time) *MockTime {
	mt := &MockTime{
		T:    t,
		Cond: sync.NewCond(&sync.Mutex{}),
	}
	return mt
}

// Now returns the stored time.Time, It is to mock out time.Now().
func (t MockTime) Now() time.Time {
	t.RLock()
	defer t.RUnlock()
	return t.T
}

// Unix creates a time.Time given seconds and nanoseconds.  It just wraps time.Unix.
func (_ MockTime) Unix(sec, nsec int64) time.Time {
	return time.Unix(sec, nsec)
}

// Util is equivalent to  t.T.Sub(ts).  We need it to mock out time, because the non-mocked implementation needs to be monotonic.
func (t MockTime) Until(ts time.Time) time.Duration {
	return t.T.Sub(ts)
}

// NewTimer returns a timer that will fire after d time.Duration from the underlying time in the MockTime.  It doesn't
// actually fire after a duration, but fires when you Set the MockTime used to create it, to a time greater than or
// equal to the underlying MockTime when it was created plus duration d.
func (t *MockTime) NewTimer(d time.Duration) Timer {
	t.RLock()
	defer t.RUnlock()
	timer := &MockTimer{
		T:        t,
		fireTime: t.T.Add(d),
		stopch:   make(chan struct{}, 1),
		c:        make(chan time.Time, 1),
	}
	go timer.start(d)
	return timer
}

func (t *MockTimer) start(ts time.Duration) {
	t.T.Lock()
	t.active = true
	t.T.Unlock()
	for {
		t.T.Cond.L.Lock()
		for t.T.Get().Before(t.fireTime) {
			t.T.Wait()
		}
		select {
		case t.c <- t.fireTime:
			log.Println("firing")
			t.T.Lock()
			t.active = false
			t.T.Unlock()
		case <-t.stopch:
			log.Println("stopping")
			t.T.Cond.L.Unlock()
			return
		default:
		}
		t.T.Cond.L.Unlock()

	}
	//for {t.T.Cond.
	//	t.T.Cond.Wait()
	//	t.T.RLock()
	//	ts := t.T.T
	//	ft := t.fireTime
	//	t.T.RUnlock()
	//	select {
	//	case <-t.stopch:
	//		t.T.Lock()
	//		t.fireTime = time.Time{}
	//		t.T.Unlock()
	//	default:
	//	}
	//	if (!ft.IsZero()) && !ft.After(ts) {
	//		select {
	//		case t.c <- ft:
	//		default:
	//		}
	//		t.T.Lock()
	//		t.fireTime = time.Time{}
	//		t.T.Unlock()
	//	}
	//}
}

// Set sets the underlying time to ts.  It is used when mocking time out.  It is threadsafe.
func (t *MockTime) Set(ts time.Time) {
	t.Lock()
	t.T = ts
	t.Cond.Broadcast()
	t.Unlock()
}

// Get gets the underlying time in a threadsafe way.
func (t *MockTime) Get() time.Time {
	t.RLock()
	defer t.RUnlock()
	return t.T
}

// MockTimer is a struct to mock out Timer.
type MockTimer struct {
	T        *MockTime
	fireTime time.Time
	c        chan time.Time
	stopch   chan struct{}
	active   bool
}

// C returns a <chan time.Time, it is analogous to time.Timer.C.
func (t *MockTimer) C() <-chan time.Time {
	return t.c
}

// Reset changes the timer to expire after duration d. It returns true if the timer had been active, false if the timer had expired or been stopped.
func (t *MockTimer) Reset(d time.Duration) bool {
	t.T.Lock()
	defer t.T.Unlock()
	t.fireTime = t.fireTime.Add(d)
	if !t.active {
		t.stopch = make(chan struct{}, 1)
		t.active = true
		go t.start(d)
		return false
	}
	fmt.Println("here 99")
	//t.T.Cond.Broadcast()

	log.Println("here here 7")
	return t.active
}

// Stop prevents the Timer from firing. It returns true if the call stops the timer, false if the timer has already
// expired or been stopped. Stop does not close the channel, to prevent a read from the channel succeeding incorrectly.
//
// To prevent a timer created with NewTimer from firing after a call to Stop, check the return value and drain the
// channel. For example, assuming the program has not received from t.C already:
//	if !t.Stop() {
//		<-t.C
//	}
//	t.Reset(d)

// This should not be done concurrent to other receives from the Timer's channel.
func (t *MockTimer) Stop() bool {
	t.T.RLock()
	defer t.T.RUnlock()
	if t.active {
		return false
	}
	select {
	case t.stopch <- struct{}{}:
		t.active = false
		return true
	default:
		return false
	}
}
