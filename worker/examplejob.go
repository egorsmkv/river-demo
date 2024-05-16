package worker

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/riverqueue/river"
)

type ExampleJobArgs struct {
	Email string `json:"email"`
}

func (ExampleJobArgs) Kind() string {
	return "example"
}

func (ExampleJobArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		UniqueOpts: river.UniqueOpts{
			ByArgs:   true,
			ByPeriod: 4 * time.Hour,
		},
	}
}

type ExampleWorker struct {
	river.WorkerDefaults[ExampleJobArgs]

	logger *slog.Logger
}

func (w *ExampleWorker) Work(
	ctx context.Context,
	job *river.Job[ExampleJobArgs],
) error {
	w.logger.Debug("ExampleWorker task started", "email", job.Args.Email)

	// Some heavy work...
	randRange := func(min, max int) int {
		return rand.IntN(max-min) + min
	}
	time.Sleep(time.Duration(randRange(1, 10)) * time.Second)

	w.logger.Debug("ExampleWorker task finished", "email", job.Args.Email)

	return nil
}

func (w *ExampleWorker) NextRetry(job *river.Job[ExampleJobArgs]) time.Time {
	return time.Now().Add(10 * time.Second)
}

func NewExampleWorker(logger *slog.Logger) *ExampleWorker {
	return &ExampleWorker{logger: logger}
}
