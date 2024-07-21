package workerpool

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"math/rand"
	"os"
	"testing"
	"time"
)

type Pool[T any] interface {
	Start() (<-chan T, <-chan error)
	Submit(func() (T, error))
	Stop()
}

func getLogger(level zapcore.Level) logr.Logger {
	encoderCfg := zap.NewDevelopmentEncoderConfig()
	encoderCfg.TimeKey = ""
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	var core zapcore.Core
	switch level {
	case zapcore.InfoLevel:
		atom := zap.NewAtomicLevel()
		core = zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderCfg),
			zapcore.Lock(os.Stdout),
			atom,
		)
		atom.SetLevel(zap.InfoLevel)
	case zapcore.ErrorLevel:
		core = zapcore.NewCore(
			zapcore.NewConsoleEncoder(encoderCfg),
			zapcore.Lock(os.Stdout),
			zap.LevelEnablerFunc(func(level zapcore.Level) bool {
				return level == zapcore.ErrorLevel
			}),
		)
	}
	zapLogger := zap.New(core)
	defer zapLogger.Sync()

	return zapr.NewLogger(zapLogger)
}

func TestWorkerPool_Succeed(t *testing.T) {
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
	}

	testLogger := getLogger(zapcore.InfoLevel).WithName("test")

	for testCount, test := range tests {
		poolName := fmt.Sprintf("pool-%d", testCount)

		var wp Pool[string]
		wp = NewWorkerPool[string](test.maxWorkerCount, getLogger(zapcore.InfoLevel).WithName(poolName))

		timer := getTimer()
		results, _ := wp.Start()

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

func TestWorkerPool_SucceedOrError(t *testing.T) {
	defer goleak.VerifyNone(t)

	tests := []struct {
		taskCount      int
		maxWorkerCount int
	}{
		{
			taskCount:      20,
			maxWorkerCount: 10,
		},
	}

	testLogger := getLogger(zapcore.InfoLevel).WithName("test")

	for testCount, test := range tests {
		poolName := fmt.Sprintf("pool-%d", testCount)

		var wp Pool[string]
		wp = NewWorkerPool[string](test.maxWorkerCount, getLogger(zapcore.ErrorLevel).WithName("pool"))

		timer := getTimer()
		results, errors := wp.Start()

		for i := 1; i <= test.taskCount; i++ {
			task := func() (string, error) {
				time.Sleep(time.Second)

				correct := (rand.Intn(10) % 2) == 0

				if !correct {
					return "", fmt.Errorf("failed task-%d", i)
				}

				return fmt.Sprintf("result of task-%d", i), nil
			}
			testLogger.Info("submit task", "id", i)
			wp.Submit(task)
		}

		for i := 0; i < test.taskCount; i++ {
			select {
			case result, ok := <-results:
				if !ok {
					testLogger.Info("results chan is closed")
					break
				}
				testLogger.Info("result received", "result", result)
			case err, ok := <-errors:
				if !ok {
					testLogger.Info("errors chan is closed")
					break
				}
				testLogger.Info("error received", "err", err)
			}
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
