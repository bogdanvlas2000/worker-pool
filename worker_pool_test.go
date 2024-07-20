package workerpool

import (
	"fmt"
	"github.com/go-logr/zapr"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"testing"
	"time"
)

type Pool[T any] interface {
	Start() <-chan T
	Submit(func() (T, error))
	Stop()
}

func TestWorkerPool_TaskExecution(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		taskCount      int
		maxWorkerCount int
	}{
		{
			//TODO: livelock with this test input (3, 1)
			taskCount:      5,
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

	for testCount, test := range tests {
		poolName := fmt.Sprintf("pool-%d", testCount)

		var wp Pool[string]
		wp = NewWorkerPool[string](test.maxWorkerCount, logger.WithName(poolName))

		timer := getTimer()
		results := wp.Start()

		for i := 1; i <= test.taskCount; i++ {
			task := func() (string, error) {
				time.Sleep(time.Second)
				return fmt.Sprintf("result of task-%d", i), nil
			}
			testLogger.Info("submit task")
			wp.Submit(task)
		}

		for i := 0; i < test.taskCount; i++ {
			testLogger.Info("attempting to receive result...")
			result, ok := <-results
			if !ok {
				testLogger.Info("results chan is closed")
				break
			}
			testLogger.Info("result received", "result", result)
		}

		testLogger.Info("stop worker pool")
		wp.Stop()
		duration := timer()

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
