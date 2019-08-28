package scheduler

import (
	"context"
	"time"

	"github.com/influxdata/cron"
	"github.com/influxdata/influxdb"
)

// ID duplicates the influxdb ID so users of the scheduler don't have to
// import influxdb for the id.
// TODO(lh): maybe make this its own thing sometime in the future.
type ID influxdb.ID

var maxID = ID(int(^uint(0) >> 1))

// Checkpointer allows us to restart a service from the last time we executed.
type Checkpointer interface {
	// Checkpoint saves the last checkpoint a id has reached.
	Checkpoint(ctx context.Context, id ID, t time.Time) error
}

// Promise is the currently executing element from the a call to Execute.
type Promise interface {
	// ID is a unique ID usable to look up or cancel a run.
	ID() ID

	// Cancel a promise, identical to calling executor.Cancel().
	Cancel(ctx context.Context)

	// Done returns a read only channel that when closed indicates the execution is complete.
	Done() <-chan struct{}

	// Error returns an error only when the execution is complete.
	// This is a hanging call until Done() is closed.
	Error() error
}

// Schedulable is a object that can be scheduled by the scheduler.
type Schedulable interface {
	// ID a unique ID we can to lookup a scheduler
	ID() ID

	// Schedule is the schedule you want execution.
	Schedule() Schedule

	// Since returns when to start scheduling.
	Since() time.Time
}

// Schedule is a timing mechanism that helps us know when the next schedulable is due.
type Schedule struct {
	Schedule string
	Offset   time.Duration
}

// Valid check to see if the schedule has a valid schedule string.
// Valid schedule strings are a cron syntax `* * * * *` or `@every 1s`.
func (s Schedule) Valid() error {
	_, err := cron.ParseUTC(s.Schedule)
	return err
}

// Next returns the next time a schedule needs to run after checkpoint time.
func (s Schedule) Next(checkpoint time.Time) (time.Time, error) {
	sch, err := cron.ParseUTC(s.Schedule)
	if err != nil {
		return time.Time{}, err
	}
	return sch.Next(checkpoint)
}

// Scheduler is a example interface of a Scheduler.
// // todo(lh): remove this once we start building the actual scheduler
// type Scheduler interface {
// 	// Start allows the scheduler to Tick. A scheduler without start will do nothing
// 	Start(ctx context.Context)

// 	// Stop a scheduler from ticking.
// 	Stop()

// 	Now() time.Time

// 	// ClaimTask begins control of task execution in this scheduler.
// 	ClaimTask(authCtx context.Context, task Schedulable) error

// 	// UpdateTask will update the concurrency and the runners for a task
// 	UpdateTask(authCtx context.Context, task Schedulable) error

// 	// ReleaseTask immediately cancels any in-progress runs for the given task ID,
// 	// and releases any resources related to management of that task.
// 	ReleaseTask(taskID ID) error
// }
