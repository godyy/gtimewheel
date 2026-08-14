package gtimewheel

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrTimeWheelNotStarted 时间轮未启动的错误.
var ErrTimeWheelNotStarted = errors.New("timewheel: time wheel is not started")

// ErrTimeWheelStopped 时间轮已停止的错误.
var ErrTimeWheelStopped = errors.New("timewheel: time wheel is stopped")

// ErrTimeWheelTicking 时间轮正在推进时间的错误.
var ErrTimeWheelTicking = errors.New("timewheel: time wheel is ticking")

// TimerID 定时器ID.
type TimerID = uint64

// LevelConfig 时间轮层级配置
type LevelConfig struct {
	// Name 层级名称.
	Name string

	// Span 时间间隔.
	Span time.Duration

	// Slots 槽位数量.
	Slots int
}

// Timer 定时器数据.
type Timer struct {
	// TID 定时器ID.
	TID TimerID

	// Value 定时器值.
	Value any
}

// TimerCallback 定时器触发回调.
type TimerCallback func(timer Timer)

// Config 时间轮配置.
type Config struct {
	// Levels 时间轮层级配置.
	Levels []LevelConfig

	// Callback 定时器触发回调.
	Callback TimerCallback
}

// validate 验证配置轮配置是否有效.
func (c *Config) validate() error {
	if c.Levels == nil {
		return errors.New("timewheel: Config: Levels is nil")
	}
	// 检查配置有效性
	for i, cfg := range c.Levels {
		if cfg.Slots <= 0 {
			return fmt.Errorf("timewheel: Config: level %s Slots must > 0", cfg.Name)
		}
		if cfg.Span <= 0 {
			return fmt.Errorf("timewheel: Config: level %s Span must > 0", cfg.Name)
		}
		// 检查层级跨度是否正确
		// 低层级跨度总合应该等于高层级的单位跨度
		if i > 0 {
			lowCfg := c.Levels[i-1]
			lowMaxSpan := lowCfg.Span * time.Duration(lowCfg.Slots)
			if cfg.Span != lowMaxSpan {
				return fmt.Errorf("timewheel: Config: span of level %s not equal max-span of level %s", cfg.Name, lowCfg.Name)
			}
		}
	}
	if c.Callback == nil {
		return errors.New("timewheel: Config: Callback is nil")
	}
	return nil
}

const (
	stateStarted = 1
	stateStopped = 2
)

// TimeWheel 非线程安全时间轮实现.
// TimeWheel创建后, 必须先 Start 后才能使用.
// Start 后才能使用的功能包括:
//   - AddTimer 添加定时器.
//   - RemoveTimer 移除定时器.
//   - Tick 推进时间.
//
// 你需要根据时间轮的最小时间间隔, 手动的调用 Tick 方法, 来推进时间.
type TimeWheel struct {
	state          int32              // 状态.
	ticks          int64              // 当前tick数，用于追踪绝对时间.
	tickSpan       time.Duration      // 时间轮 tick 时间跨度.
	tickTs         int64              // tick时间戳.
	ticking        bool               // 是否正在推进时间.
	timers         map[TimerID]*timer // 定时器映射.
	timersLock     sync.Mutex         // 定时器锁.
	levels         []*level           // 层级.
	triggerWorkers []chan *level      // 触发工作器.
	triggerStartWG sync.WaitGroup     // 触发启动等待组.
	triggerRunWG   sync.WaitGroup     // 触发运行等待组.
	triggerEndWG   sync.WaitGroup     // 触发结束等待组.
	cStopped       chan struct{}      // 停止信号.
	callback       TimerCallback      // 定时器触发回调.
}

// NewTimeWheel 创建时间轮
func NewTimeWheel(config *Config) (*TimeWheel, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	// 创建时间轮
	tw := &TimeWheel{
		tickSpan:       config.Levels[0].Span,
		timers:         make(map[TimerID]*timer),
		levels:         make([]*level, len(config.Levels)),
		triggerWorkers: make([]chan *level, len(config.Levels)),
		cStopped:       make(chan struct{}),
		callback:       config.Callback,
	}

	// 初始化层级
	for i, cfg := range config.Levels {
		spanTicks := int64(cfg.Span / tw.tickSpan)
		if cfg.Span%tw.tickSpan != 0 {
			return nil, fmt.Errorf("timewheel: NewTimeWheel: span of level %s not divisible by tickSpan", cfg.Name)
		}

		// 创建层级
		level := &level{
			spanTicks:   spanTicks,
			slots:       make([]*slot, cfg.Slots),
			currentSlot: 0,
			triggerSlot: -1,
		}
		// 初始化槽位
		for j := 0; j < cfg.Slots; j++ {
			level.slots[j] = &slot{
				timers: make(map[TimerID]*timer),
			}
		}
		tw.levels[i] = level
	}

	return tw, nil
}

// Start 启动时间轮.
// ts 为纳秒级时间戳.
func (tw *TimeWheel) Start(ts int64) {
	if tw.state >= stateStarted {
		return
	}

	// 初始化触发工作器.
	for i := range tw.triggerWorkers {
		c := make(chan *level, 1)
		tw.triggerWorkers[i] = c
		go func() {
			for {
				select {
				case level := <-c:
					tw.triggerStartWG.Wait()
					tw.triggerLevel(level)
					tw.triggerRunWG.Done()
					tw.triggerRunWG.Wait()
					level.triggerEnd()
					tw.triggerEndWG.Done()
				case <-tw.cStopped:
					return
				}
			}
		}()
	}

	tw.tickTs = ts
	tw.state = stateStarted
}

// Stop 停止时间轮.
func (tw *TimeWheel) Stop() {
	if tw.state == stateStopped || tw.ticking {
		return
	}
	close(tw.cStopped)
	tw.reset()
	tw.state = stateStopped
}

// Reset 重置时间轮.
func (tw *TimeWheel) Reset() {
	if tw.state == stateStopped || tw.ticking {
		return
	}
	tw.reset()
}

// reset 重置时间轮.
func (tw *TimeWheel) reset() {
	// 重置层级.
	for _, level := range tw.levels {
		level.reset()
	}

	// 重置其它数据.
	tw.timers = make(map[TimerID]*timer)
	tw.ticks = 0
}

// calcScheduleTicks 计算定时器的调度时间点.
func (tw *TimeWheel) calcScheduleTicks(ts int64) int64 {
	d := ts - tw.tickTs
	delay := d / int64(tw.tickSpan)
	if d%int64(tw.tickSpan) > 0 {
		delay++
	}
	if delay == 0 {
		delay = 1
	}
	return tw.ticks + delay
}

// convertPeriodToTicks 将周期转换为tick数.
func convertPeriodToTicks(period time.Duration, tickSpan time.Duration) int64 {
	ticks := int64(period / tickSpan)
	if period%tickSpan > 0 {
		ticks++
	}
	return ticks
}

// AddTimer 添加定时器.
// tid 为定时器ID，ts 为到期时刻的纳秒级时间戳，period 为周期，timerValue 为定时器值.
func (tw *TimeWheel) AddTimer(tid TimerID, ts int64, period time.Duration, value any) error {
	if tw.state != stateStarted {
		return ErrTimeWheelNotStarted
	}

	if tw.ticking {
		return ErrTimeWheelTicking
	}

	if ts < tw.tickTs {
		ts = tw.tickTs
	}

	if period < 0 {
		period = 0
	}

	if tw.timers[tid] != nil {
		return errors.New("timewheel: AddTimer: duplicate timer id")
	}

	// 将周期转换为tick数.
	periodTicks := convertPeriodToTicks(period, tw.tickSpan)

	// 创建定时器.
	timer := newTimer(tid, periodTicks, value)

	// 计算定时器的调度时间点.
	timer.scTicks = tw.calcScheduleTicks(ts)

	// 添加定时器.
	tw.timers[tid] = timer
	// 固定定时器到时间轮
	tw.pinTimer(timer)

	return nil
}

// pinTimer 将定时器固定到时间轮的层级结构中
func (tw *TimeWheel) pinTimer(timer *timer) {
	// 计算定时器的剩余延迟时间.
	remainTicks := timer.scTicks - tw.ticks

	// 定时器初始层级
	if timer.level == -1 {
		// 首次分派, 尝试对齐时间轮, 也就是当定时器被调度时, 低于当前的所有层级总是处于原点.
		// 使定时器依次从高层到低层调度时，每一层至多调度一次.
		timer.level = len(tw.levels) - 1
	}

	// 定位层级.
	var level *level
	for ; timer.level >= 0; timer.level-- {
		level = tw.levels[timer.level]
		if remainTicks >= level.spanTicks {
			break
		}
	}

	// 计算槽位跨度
	slots := int(remainTicks/level.spanTicks) - 1
	// 计算最终槽位：当前槽位 + 槽位跨度，然后对总槽位数取模
	timer.slot = (level.currentSlot + slots) % len(level.slots)

	// 将定时器添加到对应层级
	level.mtx.Lock()
	level.addTimer(timer)
	level.mtx.Unlock()
}

// RemoveTimer 从时间轮中删除指定定时器.
// 返回被删除定时器的值，以及是否删除成功.
func (tw *TimeWheel) RemoveTimer(tid TimerID) (any, bool) {
	if tw.state != stateStarted || tw.ticking {
		return nil, false
	}

	timer := tw.timers[tid]
	if timer == nil {
		return nil, false
	}
	delete(tw.timers, tid)

	// 将定时器从时间轮层级中移除.
	if timer.level != -1 {
		level := tw.levels[timer.level]
		level.removeTimer(timer)
	}

	return timer.value, true
}

// Tick 推进时间轮，处理到期定时器
func (tw *TimeWheel) Tick() {
	if tw.state != stateStarted || tw.ticking {
		return
	}

	// 推进时间轮.
	tw.ticking = true
	if !tw.advance() {
		tw.ticking = false
		return
	}

	// 等待触发结束.
	tw.triggerEndWG.Wait()
	tw.ticking = false
}

// TickTs 获取当前时间轮时间戳.
func (tw *TimeWheel) TickTs() int64 {
	return tw.tickTs
}

// TickDuration 获取当前时间轮总Tick时长.
func (tw *TimeWheel) TickDuration() time.Duration {
	return time.Duration(tw.ticks) * tw.tickSpan
}

// advance 推进时间轮.
func (tw *TimeWheel) advance() bool {
	// 重置触发层级数.
	triggerLevelAmount := 0

	// 推进当前tick数.
	tw.ticks++
	tw.tickTs += int64(tw.tickSpan)

	// 更新等待组
	tw.triggerStartWG.Add(1)
	defer tw.triggerStartWG.Done()
	tw.triggerRunWG.Add(len(tw.levels))
	tw.triggerEndWG.Add(len(tw.levels))

	// 推进层级, 获取待执行的定时器槽位.
	for i, level := range tw.levels {
		// 推进层级
		trigger := level.advance()
		if trigger {
			triggerLevelAmount++
		}

		// 判断层级是否归零.
		return2Zero := level.currentSlot == 0

		// 分配触发层级.
		if trigger {
			tw.triggerWorkers[i] <- level
		}

		// 层级归零, 才向上层推进.
		if !return2Zero {
			break
		}
	}

	// 根据已触发层级数调整等待组.
	if n := len(tw.levels) - triggerLevelAmount; n > 0 {
		tw.triggerRunWG.Add(-n)
		tw.triggerEndWG.Add(-n)
	}

	return triggerLevelAmount > 0
}

// triggerLevel 触发层级.
func (tw *TimeWheel) triggerLevel(level *level) {
	// 获取触发槽位.
	slot := level.slots[level.triggerSlot]

	// 触发槽位中的定时器.
	for tid, timer := range slot.timers {
		// 删除定时器.
		slot.remove(tid)

		// 触发定时器.
		if tw.ticks >= timer.scTicks {
			// 定时器已到期.

			// 根据定时器是否为周期性定时器做相应处理.
			if timer.period > 0 {
				// 周期性定时器, 重置状态.
				timer.level = -1
				timer.slot = -1
				timer.scTicks = tw.ticks + timer.period
				tw.pinTimer(timer)
			} else {
				// 非周期性定时器直接删除.
				tw.timersLock.Lock()
				delete(tw.timers, timer.id)
				tw.timersLock.Unlock()
			}

			// 执行定时器触发回调.
			tw.callback(Timer{
				TID:   timer.id,
				Value: timer.value,
			})

		} else {
			// 定时器未到期, 重新调度.

			tw.pinTimer(timer)
		}
	}
}

// timer 定时器.
type timer struct {
	id      TimerID // 定时器ID.
	period  int64   // 周期Ticks.
	value   any     // 定时器值.
	level   int     // 所在层级.
	slot    int     // 所在槽位.
	scTicks int64   // 调度时间点, tick 数.
}

func newTimer(id TimerID, period int64, value any) *timer {
	t := &timer{}
	t.id = id
	t.period = period
	t.value = value
	t.level = -1
	t.slot = -1
	t.scTicks = 0
	return t
}

// slot 槽位.
type slot struct {
	timers map[TimerID]*timer // 定时器.
}

func (s *slot) add(t *timer) {
	s.timers[t.id] = t
}

func (s *slot) remove(tid TimerID) {
	delete(s.timers, tid)
}

func (s *slot) empty() bool {
	return len(s.timers) == 0
}

func (s *slot) reset() {
	if len(s.timers) == 0 {
		return
	}
	s.timers = make(map[TimerID]*timer)
}

// level 层级结构.
type level struct {
	spanTicks int64   // 时间跨度Ticks.
	slots     []*slot // 槽位.

	mtx         sync.RWMutex       // 读写锁.
	currentSlot int                // 当前槽位.
	triggerSlot int                // 正在触发的槽位. -1 表示没有触发. 正在触发的槽位无法添加定时器.
	delayTimers map[TimerID]*timer // 延迟定时器列表, 表示需要在当前触发槽位触发完毕后添加到其中的的定时器.
}

// addTimer 添加定时器
// 如果槽位正在触发, 则将定时器添加到延迟定时器列表中.
// 否则, 直接添加到槽位.
func (l *level) addTimer(t *timer) {
	if t.slot == l.triggerSlot {
		if l.delayTimers == nil {
			l.delayTimers = make(map[TimerID]*timer)
		}
		l.delayTimers[t.id] = t
		return
	}

	l.slots[t.slot].add(t)
}

// removeTimer 移除定时器
// 从槽位中移除定时器.
func (l *level) removeTimer(t *timer) {
	l.slots[t.slot].remove(t.id)
}

// advance 推进.
func (l *level) advance() (trigger bool) {
	l.mtx.Lock()
	defer l.mtx.Unlock()

	// 更新触发槽位.
	slot := l.slots[l.currentSlot]
	if !slot.empty() {
		// 槽位非空, 添加触发槽位, 并设置层级触发槽位.
		l.triggerSlot = l.currentSlot
		trigger = true
	}

	// 更新层级当前槽位.
	l.currentSlot = (l.currentSlot + 1) % len(l.slots)

	return
}

// triggerEnd 层级触发结束.
func (l *level) triggerEnd() {
	l.triggerSlot = -1
	if l.delayTimers != nil {
		for _, timer := range l.delayTimers {
			l.addTimer(timer)
		}
		l.delayTimers = nil
	}
}

func (l *level) reset() {
	for i := range l.slots {
		l.slots[i].reset()
	}
	l.currentSlot = 0
	l.triggerSlot = -1
	l.delayTimers = nil
}
