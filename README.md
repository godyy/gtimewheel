## 用例
```go
import (
    "log"
    "sync"
    "time"

    "github.com/godyy/gtimewheel"
)

func main() {
    levelConfigs := []gtimewheel.LevelConfig{
        {Name:"100ms", Span:100*time.Millisecond, Slots: 10},
        {Name:"s", Span:1 * time.Second, Slots: 60},
        {Name:"m", Span:1 * time.Minute, Slots: 60},
        {Name:"h", Span:1 * time.Hour, Slots: 24},
    }
    executor := func(f TimerFunc, args TimerArgs) {
        go f(args)
    }
    timewheel, err := gtimewheel.NewTimeWheel(levelConfigs, executor)
    if err != nil {
        log.Fatal(err)
    }

    wg := sync.WaitGroup{}
    timerId, err := timewheel.AddTimer(gtimewheel.TimerOptions{
        Delay: 1 * time.Seoncd,
        Offset: 0,
        Periodic: false,
        Func: func(args TimerArgs){
            wg.Done()
        },
        Args: nil,
    })
    if err != nil {
        log.Fatal(err)
    }
    wg.Add(1)

    go func() {
        ticker := time.NewTicker(levelConfigs[0].Span)
        for {
            select {
            case <-ticker.C:
                timewheel.Tick(1)
            }
        }
    }

    wg.Wait()
}

```