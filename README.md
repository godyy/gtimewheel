## 简介

`gtimewheel` 是一个分层时间轮实现。

它的公开 API 按非线程安全方式设计：`Start`、`AddTimer`、`RemoveTimer`、`Tick`、`Reset`、`Stop` 需要由调用方串行调用。

`Tick()` 内部会并发触发不同层级的槽位，但会在内部全部处理完成后才返回。

## 用例

```go
package main

import (
	"log"
	"sync"
	"time"

	"github.com/godyy/gtimewheel"
)

func main() {
	config := &gtimewheel.Config{
		Levels: []gtimewheel.LevelConfig{
			{Name: "100ms", Span: 100 * time.Millisecond, Slots: 10},
			{Name: "s", Span: time.Second, Slots: 60},
			{Name: "m", Span: time.Minute, Slots: 60},
			{Name: "h", Span: time.Hour, Slots: 24},
		},
		Executor: func(f gtimewheel.TimerFunc, args gtimewheel.TimerArgs) {
			// 如需异步执行回调，可以在这里起 goroutine。
			f(args)
		},
	}

	tw, err := gtimewheel.NewTimeWheel(config)
	if err != nil {
		log.Fatal(err)
	}

	ts := time.Now().UnixNano()
	tw.Start(ts)
	defer tw.Stop()

	var wg sync.WaitGroup
	wg.Add(1)

	err = tw.AddTimer(1, ts+int64(time.Second), 0, func(args gtimewheel.TimerArgs) {
		log.Printf("timer %d fired", args.TID)
		wg.Done()
	}, nil)
	if err != nil {
		log.Fatal(err)
	}

	ticker := time.NewTicker(config.Levels[0].Span)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			tw.Tick()
		case <-waitDone(&wg):
			return
		}
	}
}

func waitDone(wg *sync.WaitGroup) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}
```

## API 说明

- `Start(ts)`：启动时间轮，`ts` 为纳秒时间戳。
- `AddTimer(tid, ts, period, f, args)`：添加定时器。
- `period == 0` 表示一次性定时器，`period > 0` 表示周期性定时器。
- `Tick()`：推进一个最小时间粒度，并在返回前完成本次内部触发。
- `Reset()`：清空定时器和 tick 状态，但不会停止时间轮。
- `Stop()`：停止时间轮并清空内部状态。

## 使用约束

- 必须先 `Start()`，再调用 `AddTimer()`、`RemoveTimer()`、`Tick()`。
- `Tick()` 执行期间，不应再对同一个时间轮调用 `AddTimer()`、`RemoveTimer()`、`Reset()`、`Stop()`。
- `AddTimer()` 中的 `ts` 如果早于当前时间轮时间，会按当前时间处理，并在下一次 `Tick()` 时触发。
