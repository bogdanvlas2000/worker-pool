package worker_pool

import (
	"fmt"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"testing"
	"time"
)

func TestWorkerPool_TaskExecution(t *testing.T) {
	tests := []struct {
		taskCount      int
		maxWorkerCount int
	}{
		{
			taskCount:      2,
			maxWorkerCount: 2,
		},
		//{
		//	taskCount:      5,
		//	maxWorkerCount: 2,
		//},
		//{
		//	taskCount:      2,
		//	maxWorkerCount: 5,
		//},
	}

	atom := zap.NewAtomicLevel()

	encoderCfg := zap.NewDevelopmentEncoderConfig()
	encoderCfg.TimeKey = ""
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	zapLogger := zap.New(zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		atom,
	))

	defer zapLogger.Sync()

	atom.SetLevel(zap.InfoLevel)

	logger := zapr.NewLogger(zapLogger)

	testLogger := logger.WithName("test")

	for i, test := range tests {
		poolName := fmt.Sprintf("pool-%d", i)
		wp := NewWorkerPool(test.maxWorkerCount, logger.WithName(poolName))

		timer := getTimer()
		results := wp.Start()

		for i := 1; i <= test.taskCount; i++ {
			task := Task{
				ID: i,
			}
			testLogger.Info("submit task", "taskId", task)
			wp.Submit(task)
		}
		duration := timer()

		for i := 0; i < test.taskCount; i++ {
			result, ok := <-results
			if !ok {
				testLogger.Info("results chan is closed")
				break
			}
			testLogger.Info("result received", "value", result.Value)
		}

		testLogger.Info("stop worker pool")
		wp.Stop()

		testLogger.Info("test completed", "workerPool", poolName, "duration", duration)
		fmt.Println()
	}
}

func getTimer() func() time.Duration {
	start := time.Now()
	return func() time.Duration {
		return time.Since(start)
	}
}
