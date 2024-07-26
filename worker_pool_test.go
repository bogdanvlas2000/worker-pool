package workerpool

import (
	"fmt"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"testing"
)

type TestPool[T any] interface {
	Start() (<-chan T, <-chan error)
	Submit(func() (T, error))
	Stop()
}

func getTestLogger(level zapcore.Level) logr.Logger {
	encoderCfg := zap.NewDevelopmentEncoderConfig()
	encoderCfg.TimeKey = ""
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	core := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		zap.LevelEnablerFunc(func(level zapcore.Level) bool {
			return level == level
		}),
	)

	zapLogger := zap.New(core)
	defer zapLogger.Sync()

	return zapr.NewLogger(zapLogger)
}

func TestWorkerPool_New(t *testing.T) {
	tests := map[string]struct {
		opts                    []Opt[string]
		expectedMaxWorkerCount  uint32
		expectedLoggerVerbosity int
	}{
		"should create worker pool with default params": {
			opts:                    []Opt[string]{},
			expectedMaxWorkerCount:  DefaultMaxWorkersCount,
			expectedLoggerVerbosity: defaultLogger().GetV(),
		},
		"should create worker pool with custom params": {
			opts: []Opt[string]{
				WithMaxWorkerCount[string](5),
				WithLogger[string](getTestLogger(zapcore.InfoLevel)),
			},
			expectedMaxWorkerCount:  5,
			expectedLoggerVerbosity: getTestLogger(zapcore.InfoLevel).GetV(),
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			wp := New[string](test.opts...)
			assert.Equal(t, test.expectedMaxWorkerCount, wp.MaxWorkerCount())
			assert.Equal(t, test.expectedLoggerVerbosity, wp.Logger().GetV())
		})
	}
}

func TestWorkerPool_worker_single_task(t *testing.T) {
	defer goleak.VerifyNone(t)

	l := getTestLogger(zapcore.InfoLevel).WithName("test")

	testResult := "test result"
	testErr := fmt.Errorf("test err")

	tests := map[string]struct {
		task           func() (string, error)
		expectedResult string
		expectedError  error
	}{
		"should return result": {
			task: func() (string, error) {
				return testResult, nil
			},
			expectedResult: testResult,
			expectedError:  nil,
		},
		"should return error": {
			task: func() (string, error) {
				return "", testErr
			},
			expectedResult: "",
			expectedError:  testErr,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			wp := New[string]()

			wp.worker(0)

			wp.tasksToExecute <- test.task

			var result string
			var err error
			select {
			case result = <-wp.resultQueue:
				l.Info("result received", "result", result)
			case err = <-wp.errorQueue:
				l.Error(err, "error received")
			}

			assert.Equal(t, test.expectedResult, result)
			assert.Equal(t, test.expectedError, err)

			close(wp.stopSignal)
		})
	}
}

//func TestWorkerPool_Succeed(t *testing.T) {
//	defer goleak.VerifyNone(t)
//
//	tests := []struct {
//		taskCount      int
//		maxWorkerCount int
//	}{
//		{
//			taskCount:      5,
//			maxWorkerCount: 2,
//		},
//	}
//
//	testLogger := getTestLogger(zapcore.InfoLevel).WithName("test")
//
//	for testCount, test := range tests {
//		poolName := fmt.Sprintf("pool-%d", testCount)
//
//		var wp TestPool[string]
//		wp = New[string](test.maxWorkerCount, getTestLogger(zapcore.InfoLevel).WithName(poolName))
//
//		timer := getTimer()
//		results, _ := wp.Start()
//
//		for i := 1; i <= test.taskCount; i++ {
//			task := func() (string, error) {
//				time.Sleep(time.Second)
//				return fmt.Sprintf("result of task-%d", i), nil
//			}
//			testLogger.Info("submit task")
//			wp.Submit(task)
//		}
//
//		for i := 0; i < test.taskCount; i++ {
//			testLogger.Info("attempting to receive result...")
//			result, ok := <-results
//			if !ok {
//				testLogger.Info("results chan is closed")
//				break
//			}
//			testLogger.Info("result received", "result", result)
//		}
//
//		testLogger.Info("stop worker pool")
//		wp.Stop()
//		duration := timer()
//
//		testLogger.Info("test completed", "workerPool", poolName, "duration", duration)
//		fmt.Println()
//	}
//}
//
//func TestWorkerPool_SucceedOrError(t *testing.T) {
//	defer goleak.VerifyNone(t)
//
//	tests := []struct {
//		taskCount      int
//		maxWorkerCount int
//	}{
//		{
//			taskCount:      20,
//			maxWorkerCount: 10,
//		},
//	}
//
//	testLogger := getTestLogger(zapcore.InfoLevel).WithName("test")
//
//	for testCount, test := range tests {
//		poolName := fmt.Sprintf("pool-%d", testCount)
//
//		var wp TestPool[string]
//		wp = New[string](test.maxWorkerCount, getTestLogger(zapcore.ErrorLevel).WithName("pool"))
//
//		timer := getTimer()
//		results, errors := wp.Start()
//
//		for i := 1; i <= test.taskCount; i++ {
//			task := func() (string, error) {
//				time.Sleep(time.Second)
//
//				correct := (rand.Intn(10) % 2) == 0
//
//				if !correct {
//					return "", fmt.Errorf("failed task-%d", i)
//				}
//
//				return fmt.Sprintf("result of task-%d", i), nil
//			}
//			testLogger.Info("submit task", "id", i)
//			wp.Submit(task)
//		}
//
//		for i := 0; i < test.taskCount; i++ {
//			select {
//			case result, ok := <-results:
//				if !ok {
//					testLogger.Info("results chan is closed")
//					break
//				}
//				testLogger.Info("result received", "result", result)
//			case err, ok := <-errors:
//				if !ok {
//					testLogger.Info("errors chan is closed")
//					break
//				}
//				testLogger.Info("error received", "err", err)
//			}
//		}
//
//		testLogger.Info("stop worker pool")
//		wp.Stop()
//		duration := timer()
//
//		testLogger.Info("test completed", "workerPool", poolName, "duration", duration)
//		fmt.Println()
//	}
//}
//
//func getTimer() func() time.Duration {
//	start := time.Now()
//	return func() time.Duration {
//		return time.Since(start)
//	}
//}
