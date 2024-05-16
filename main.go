package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"river-demo/worker"

	"github.com/integrii/flaggy"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lmittmann/tint"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

var (
	argDoMigrate    bool
	argProduceTasks bool
)

type api struct {
	client *river.Client[pgx.Tx]
	pool   *pgxpool.Pool
	logger *slog.Logger
	ctx    context.Context
}

func (a *api) insertJob(args worker.ExampleJobArgs) (*rivertype.JobInsertResult, error) {
	tx, err := a.pool.Begin(a.ctx)
	if err != nil {
		return nil, err
	}
	defer func(tx pgx.Tx, ctx context.Context) {
		err := tx.Rollback(ctx)
		if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			a.logger.Error("Cannot rollback a transaction", "error", err)
		}
	}(tx, a.ctx)

	insertResult, err := a.client.InsertTx(a.ctx, tx, args, nil)
	if err != nil {
		return nil, err
	}

	if err = tx.Commit(a.ctx); err != nil {
		return nil, err
	}

	return insertResult, nil
}

func parseArgs() {
	flaggy.SetName("rivder-demo")
	flaggy.SetDescription("A program to test github.com/riverqueue/river")
	flaggy.SetVersion("1.0")

	flaggy.Bool(&argDoMigrate, "g", "migrate", "Perform migration")
	flaggy.Bool(&argProduceTasks, "p", "produce", "Produce tasks")

	flaggy.Parse()
}

func main() {
	parseArgs()

	logger := slog.New(
		tint.NewHandler(os.Stderr, &tint.Options{
			Level:      slog.LevelDebug,
			TimeFormat: time.RFC3339,
		}),
	)

	ctx := context.Background()

	config, err := pgxpool.ParseConfig(
		fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", "postgres", "postgres", "localhost", "5432", "postgres"),
	)
	if err != nil {
		logger.Error("Cannot parse the config", "error", err)
		os.Exit(1)
	}

	config.MaxConnLifetime = 1 * time.Hour
	config.MaxConnIdleTime = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		logger.Error("Cannot create a PgSQL pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if argDoMigrate {
		migrator := rivermigrate.New(riverpgxv5.New(pool), nil)
		if _, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, &rivermigrate.MigrateOpts{}); err != nil {
			logger.Error("Cannot make migrations", "error", err)
			os.Exit(1)
		}

		logger.Info("Migrated successfully")

		os.Exit(0)
	}

	workers := river.NewWorkers()

	river.AddWorker(workers, worker.NewExampleWorker(logger))

	client, err := river.NewClient[pgx.Tx](riverpgxv5.New(pool), &river.Config{
		Logger: logger,
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 20},
		},
		Workers: workers,
	})
	if err != nil {
		logger.Error("Cannot create the client", "error", err)
		os.Exit(1)
	}

	nTasks := 5

	a := &api{
		client: client,
		pool:   pool,
		logger: logger,
		ctx:    ctx,
	}

	if argProduceTasks {
		// produce tasks each 30s
		go func() {
			for {
				go produceTasks(a, nTasks)

				time.Sleep(30 * time.Second)
			}
		}()
	} else {
		if err := a.client.Start(ctx); err != nil {
			a.logger.Error("Cannot start the client", "error", err)
		}

		a.logger.Info("Client has been started")
	}

	// Wait for Ctrl+C
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)
	logger.Info("Blocking, press ctrl+c to continue...")
	<-done

	if err = client.Stop(ctx); err != nil {
		logger.Error("Cannot stop the client", "error", err)
	}
}

func produceTasks(a *api, nTasks int) {
	randRange := func(min, max int) int {
		return rand.IntN(max-min) + min
	}

	for i := 0; i <= nTasks; i++ {
		r := randRange(1000, 9999)
		testEmail := fmt.Sprintf("test_%d@example.com", r)
		res, err := a.insertJob(worker.ExampleJobArgs{Email: testEmail})
		if err != nil {
			a.logger.Error("Cannot insert a job", "error", err)
		} else {
			a.logger.Info("Inserted a task", "email", testEmail, "job_id", res.Job.ID)
		}
	}
}
