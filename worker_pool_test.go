package worker_pool

import (
	"flag"
	"fmt"
	"go.uber.org/zap/zapcore"
	"log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	tests := []struct {
		taskCount      int
		maxWorkerCount int
	}{
		{
			taskCount:      2,
			maxWorkerCount: 2,
		},
		{
			taskCount:      5,
			maxWorkerCount: 2,
		},
		{
			taskCount:      2,
			maxWorkerCount: 5,
		},
	}

	opts := &zap.Options{
		Development: true,
		TimeEncoder: zapcore.TimeEncoderOfLayout(time.RFC3339Nano),
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	logger := zap.New(zap.UseFlagOptions(opts))

	for i, test := range tests {
		tasks := []Task{}

		for i := 0; i < test.taskCount; i++ {
			tasks = append(tasks, Task{
				ID: i,
			})
		}

		poolName := fmt.Sprintf("pool-%d", i)
		pool, err := NewWorkerPool(tasks, test.maxWorkerCount, logger.WithName(poolName))
		if err != nil {
			log.Fatalln(err)
		}

		timer := getTimer()
		pool.Start()
		duration := timer()

		logger.Info("test completed", "workerPool", poolName, "duration", duration)
		fmt.Println()

	}
}

func getTimer() func() time.Duration {
	start := time.Now()
	return func() time.Duration {
		return time.Since(start)
	}
}
