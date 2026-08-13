package gtimewheel

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testTimerId uint64

func genTestTimerId() TimerID {
	return atomic.AddUint64(&testTimerId, 1)
}

// 默认执行器
func defaultExecutor(f TimerFunc, args TimerArgs) {
	f(args)
}

func TestNewTimeWheel(t *testing.T) {
	// 秒、分、时三层
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60}, // 1分钟
		{Name: "minute", Span: time.Minute, Slots: 60}, // 1小时
		{Name: "hour", Span: time.Hour, Slots: 24},     // 24小时
	}

	_, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	// 测试无效配置
	invalidConfigs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 0}, // 无效的槽位数
	}
	_, err = NewTimeWheel(&Config{
		Levels:   invalidConfigs,
		Executor: defaultExecutor,
	})
	if err == nil {
		t.Error("Expected error for invalid slots, got nil")
	}

	invalidConfigs = []LevelConfig{
		{Name: "second", Span: 0, Slots: 60}, // 无效的时间跨度
	}
	_, err = NewTimeWheel(&Config{
		Levels:   invalidConfigs,
		Executor: defaultExecutor,
	})
	if err == nil {
		t.Error("Expected error for invalid span, got nil")
	}

	// 测试层级跨度配置
	invalidLevelConfigs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: 2 * time.Minute, Slots: 60}, // 无效的层级跨度
	}
	_, err = NewTimeWheel(&Config{
		Levels:   invalidLevelConfigs,
		Executor: defaultExecutor,
	})
	if err == nil {
		t.Error("Expected error for invalid level span, got nil")
	}

	// 测试nil执行器
	_, err = NewTimeWheel(&Config{
		Levels:   configs,
		Executor: nil,
	})
	if err == nil {
		t.Error("Expected error for nil executor, got nil")
	}
}

func TestTimeWheelBasic(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)

	// 测试添加无效定时器
	err = tw.AddTimer(genTestTimerId(), ts, 0, func(args TimerArgs) {}, nil)

	err = tw.AddTimer(genTestTimerId(), ts+int64(25*time.Hour), 0, func(args TimerArgs) {}, nil)

	err = tw.AddTimer(genTestTimerId(), ts+int64(time.Second), 0, nil, nil)
	if err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestTimeWheelTimerExecution(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)

	// 测试一次性定时器
	wg := &sync.WaitGroup{}
	wg.Add(1)
	timerExecuted := false

	err = tw.AddTimer(genTestTimerId(), ts+int64(2*time.Second), 0, func(args TimerArgs) {
		timerExecuted = true
		wg.Done()
	}, nil)
	if err != nil {
		t.Fatalf("Failed to add timer: %v", err)
	}

	// 推进时间
	for i := 0; i < 2; i++ {
		tw.Tick()
	}

	// 等待定时器执行
	wg.Wait()
	if !timerExecuted {
		t.Error("Timer was not executed")
	}

	// 测试定时器删除
	timerId := genTestTimerId()
	err = tw.AddTimer(timerId, tw.TickTs()+int64(2*time.Second), 0, func(args TimerArgs) {
		t.Error("Timer should not be executed")
	}, nil)
	if err != nil {
		t.Fatalf("Failed to add timer: %v", err)
	}

	if !tw.RemoveTimer(timerId) {
		t.Error("Failed to remove timer")
	}

	for i := 0; i < 2; i++ {
		tw.Tick()
	}
}

func TestTimeWheelPeriodicTimer(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)

	// 测试周期性定时器
	var wg sync.WaitGroup
	executionCount := 0
	expectedExecutions := 3

	wg.Add(expectedExecutions)
	err = tw.AddTimer(genTestTimerId(), ts+int64(time.Second), time.Second, func(args TimerArgs) {
		executionCount++
		wg.Done()
	}, nil)
	if err != nil {
		t.Fatalf("Failed to add periodic timer: %v", err)
	}

	// 推进时间并等待执行
	for i := 0; i < expectedExecutions; i++ {
		tw.Tick()
		time.Sleep(100 * time.Millisecond) // 添加小延迟确保定时器执行完成
	}

	wg.Wait()
	if executionCount != expectedExecutions {
		t.Errorf("Expected %d executions, got %d", expectedExecutions, executionCount)
	}
}

func TestTimeWheelMultipleTimers(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)

	// 测试多个定时器
	var wg sync.WaitGroup
	executionCount := int32(0)
	timerCount := 5

	wg.Add(timerCount)
	for i := 0; i < timerCount; i++ {
		err = tw.AddTimer(genTestTimerId(), ts+int64(i+1)*int64(time.Second), 0, func(args TimerArgs) {
			atomic.AddInt32(&executionCount, 1)
			wg.Done()
		}, nil)
		if err != nil {
			t.Fatalf("Failed to add timer %d: %v", i, err)
		}
	}

	// 推进时间并等待所有定时器执行
	for i := 0; i < timerCount; i++ {
		tw.Tick()
	}

	wg.Wait()
	if int(executionCount) != timerCount {
		t.Errorf("Expected %d executions, got %d", timerCount, executionCount)
	}
}

func TestTimeWheelRandomTimers(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)

	const totalTicks = 2000000 // 大约5.5小时的tick数
	var executedTimers int32
	var addedTimers int32

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < totalTicks; i++ {
		// 50%概率添加新定时器
		if r.Float64() < 0.5 {
			remainingTicks := totalTicks - i

			// 在10分钟到5小时之间随机
			minDelay := 10 * time.Minute
			maxDelay := 5 * time.Hour
			delay := minDelay + time.Duration(r.Int63n(int64(maxDelay-minDelay)))

			// 如果随机出来的延时大于剩余时间，则修正延时
			if delay > time.Duration(remainingTicks)*time.Second {
				randomReduce := time.Duration(r.Intn(4)+1) * time.Second
				if randomReduce >= time.Duration(remainingTicks)*time.Second {
					delay = time.Duration(remainingTicks) * time.Second
				} else {
					delay = time.Duration(remainingTicks)*time.Second - randomReduce
				}
			}

			ts := tw.TickTs()
			err = tw.AddTimer(genTestTimerId(), ts+int64(delay), 0, func(args TimerArgs) {
				atomic.AddInt32(&executedTimers, 1)
			}, nil)
			if err != nil {
				t.Errorf("Failed to add timer at tick %d: %v", i, err)
				continue
			}
			atomic.AddInt32(&addedTimers, 1)
		}

		// 推进时间轮
		tw.Tick()
	}

	addedCount := atomic.LoadInt32(&addedTimers)
	executedCount := atomic.LoadInt32(&executedTimers)

	if executedCount != addedCount {
		t.Errorf("Not all timers were executed. Added: %d, Executed: %d", addedCount, executedCount)
	}
}

func TestTimeWheelReset(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)

	// 添加一些定时器
	var wg sync.WaitGroup
	timerCount := 5
	wg.Add(timerCount)

	// 添加一次性定时器
	for i := 0; i < timerCount; i++ {
		err = tw.AddTimer(genTestTimerId(), ts+int64(i+1)*int64(time.Second), 0, func(args TimerArgs) {
			wg.Done()
		}, nil)
		if err != nil {
			t.Fatalf("Failed to add timer %d: %v", i, err)
		}
	}

	// 添加一个周期性定时器
	periodicTimerId := genTestTimerId()
	err = tw.AddTimer(periodicTimerId, ts+int64(time.Second), 0, func(args TimerArgs) {}, nil)
	if err != nil {
		t.Fatalf("Failed to add periodic timer: %v", err)
	}

	// 推进一些时间
	for i := 0; i < 3; i++ {
		tw.Tick()
	}

	// 重置时间轮
	tw.Reset()
	ts = tw.TickTs()

	// 验证重置后的状态
	if tw.ticks != 0 {
		t.Errorf("Expected ticks to be 0 after reset, got %d", tw.ticks)
	}

	if tw.TickTime() != 0 {
		t.Errorf("Expected totalTickTime to be 0 after reset, got %v", tw.TickTime())
	}

	// 验证定时器池是否被清空
	if len(tw.timers) > 0 {
		t.Errorf("Expected timers to be empty after reset, got %d timers", len(tw.timers))
	}

	// 验证周期性定时器是否被清除
	if tw.RemoveTimer(periodicTimerId) {
		t.Error("Expected periodic timer to be removed after reset")
	}

	// 验证是否可以添加新定时器
	err = tw.AddTimer(genTestTimerId(), ts+int64(time.Second), 0, func(args TimerArgs) {}, nil)
	if err != nil {
		t.Errorf("Failed to add new timer after reset: %v", err)
	}

	// 验证新定时器是否可以被正确执行
	var execWg sync.WaitGroup
	execWg.Add(1)
	err = tw.AddTimer(genTestTimerId(), ts+int64(time.Second), 0, func(args TimerArgs) {
		execWg.Done()
	}, nil)
	if err != nil {
		t.Fatalf("Failed to add test timer after reset: %v", err)
	}

	tw.Tick()
	execWg.Wait()
}

func TestTimeWheelTickingGuards(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)

	// 先放入一个普通定时器，供回调里测试 RemoveTimer 使用。
	blockedTimerID := genTestTimerId()
	err = tw.AddTimer(blockedTimerID, ts+int64(10*time.Second), 0, func(args TimerArgs) {}, nil)
	if err != nil {
		t.Fatalf("Failed to add blocked timer: %v", err)
	}

	var addErr error
	var removeOK bool
	callbackDone := make(chan struct{})

	err = tw.AddTimer(genTestTimerId(), ts+int64(time.Second), 0, func(args TimerArgs) {
		// 回调在 Tick 内执行，此时对同一个时间轮的公开操作都应该被拒绝。
		addErr = tw.AddTimer(genTestTimerId(), tw.TickTs()+int64(time.Second), 0, func(args TimerArgs) {}, nil)
		removeOK = tw.RemoveTimer(blockedTimerID)
		tw.Reset()
		tw.Stop()
		close(callbackDone)
	}, nil)
	if err != nil {
		t.Fatalf("Failed to add guard timer: %v", err)
	}

	tw.Tick()

	select {
	case <-callbackDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Callback did not finish in time")
	}

	if !errors.Is(addErr, ErrTimeWheelTicking) {
		t.Fatalf("Expected ErrTimeWheelTicking, got %v", addErr)
	}

	if removeOK {
		t.Fatal("Expected RemoveTimer to fail while ticking")
	}

	// Reset/Stop 在 ticking 期间应被忽略，因此状态和已有定时器都应保持不变。
	if tw.state != stateStarted {
		t.Fatalf("Expected time wheel to remain started, got state %d", tw.state)
	}
	if tw.ticks != 1 {
		t.Fatalf("Expected ticks to remain 1 after ignored Reset/Stop, got %d", tw.ticks)
	}
	if tw.timers[blockedTimerID] == nil {
		t.Fatal("Expected blocked timer to remain after ignored operations")
	}
}

func TestTimeWheelStartIdempotent(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)

	workers := make([]chan *level, len(tw.triggerWorkers))
	copy(workers, tw.triggerWorkers)

	tw.Start(ts + int64(10*time.Second))

	// 重复 Start 应被忽略，不应重建 worker，也不应重置起始时间戳。
	if tw.TickTs() != ts {
		t.Fatalf("Expected tickTs to remain %d, got %d", ts, tw.TickTs())
	}
	for i := range workers {
		if workers[i] != tw.triggerWorkers[i] {
			t.Fatalf("Expected worker %d to remain unchanged", i)
		}
	}
}

func TestTimeWheelStop(t *testing.T) {
	configs := []LevelConfig{
		{Name: "second", Span: time.Second, Slots: 60},
		{Name: "minute", Span: time.Minute, Slots: 60},
		{Name: "hour", Span: time.Hour, Slots: 24},
	}

	tw, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)

	timerID := genTestTimerId()
	err = tw.AddTimer(timerID, ts+int64(2*time.Second), 0, func(args TimerArgs) {}, nil)
	if err != nil {
		t.Fatalf("Failed to add timer before stop: %v", err)
	}

	// 先推进一次，确保 Stop 前已有运行态数据。
	tw.Tick()
	stoppedTickTs := tw.TickTs()

	tw.Stop()

	// Stop 应清空运行态数据并将状态置为 stopped。
	if tw.state != stateStopped {
		t.Fatalf("Expected stateStopped, got %d", tw.state)
	}
	if tw.ticks != 0 {
		t.Fatalf("Expected ticks to be reset after stop, got %d", tw.ticks)
	}
	if tw.TickTime() != 0 {
		t.Fatalf("Expected TickTime to be reset after stop, got %v", tw.TickTime())
	}
	if len(tw.timers) != 0 {
		t.Fatalf("Expected timers to be cleared after stop, got %d", len(tw.timers))
	}

	// 停止后各公开操作都应保持停止态，不再推进也不能继续添加/删除。
	if err := tw.AddTimer(genTestTimerId(), stoppedTickTs+int64(time.Second), 0, func(args TimerArgs) {}, nil); !errors.Is(err, ErrTimeWheelNotStarted) {
		t.Fatalf("Expected ErrTimeWheelNotStarted after stop, got %v", err)
	}
	if tw.RemoveTimer(timerID) {
		t.Fatal("Expected RemoveTimer to fail after stop")
	}

	tw.Tick()
	if tw.TickTs() != stoppedTickTs {
		t.Fatalf("Expected TickTs to remain %d after stop, got %d", stoppedTickTs, tw.TickTs())
	}

	tw.Reset()
	if tw.state != stateStopped {
		t.Fatalf("Expected state to remain stopped after Reset, got %d", tw.state)
	}
	if tw.ticks != 0 {
		t.Fatalf("Expected ticks to remain 0 after Reset on stopped wheel, got %d", tw.ticks)
	}

	tw.Start(ts + int64(10*time.Second))
	if tw.state != stateStopped {
		t.Fatalf("Expected state to remain stopped after Start, got %d", tw.state)
	}
	if tw.TickTs() != stoppedTickTs {
		t.Fatalf("Expected TickTs to remain %d after Start on stopped wheel, got %d", stoppedTickTs, tw.TickTs())
	}
}

func TestTimeWheelTimerExceedsWheelLimit(t *testing.T) {
	configs := []LevelConfig{
		{Name: "1ms", Span: time.Millisecond, Slots: 10},
		{Name: "10ms", Span: 10 * time.Millisecond, Slots: 10},
		{Name: "100ms", Span: 100 * time.Millisecond, Slots: 10},
	}

	tw, err := NewTimeWheel(&Config{
		Levels:   configs,
		Executor: defaultExecutor,
	})
	if err != nil {
		t.Fatalf("Failed to create time wheel: %v", err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)

	// 顶层时间轮单轮容量为 100ms * 10 = 1s。
	// 这里故意设置一个远大于单轮上限的延迟，验证定时器经过多次重新分派后仍能正确触发。
	delay := 3500 * time.Millisecond
	expectedTicks := int(delay / time.Millisecond)

	var fired bool
	var firedTick int64

	err = tw.AddTimer(genTestTimerId(), ts+int64(delay), 0, func(args TimerArgs) {
		fired = true
		firedTick = tw.ticks
	}, nil)
	if err != nil {
		t.Fatalf("Failed to add timer: %v", err)
	}

	for i := 0; i < expectedTicks-1; i++ {
		tw.Tick()
		if fired {
			t.Fatalf("Timer fired too early at tick %d, expected tick %d", firedTick, expectedTicks)
		}
	}

	tw.Tick()
	if !fired {
		t.Fatalf("Expected timer to fire after %d ticks", expectedTicks)
	}
	if firedTick != int64(expectedTicks) {
		t.Fatalf("Expected timer to fire at tick %d, got %d", expectedTicks, firedTick)
	}
}
