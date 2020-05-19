/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package leaderelection implements leader election of a set of endpoints.
// It uses an annotation in the endpoints object to store the record of the
// election state. This implementation does not guarantee that only one
// client is acting as a leader (a.k.a. fencing).
//
// A client only acts on timestamps captured locally to infer the state of the
// leader election. The client does not consider timestamps in the leader
// election record to be accurate because these timestamps may not have been
// produced by a local clock. The implemention does not depend on their
// accuracy and only uses their change to indicate that another client has
// renewed the leader lease. Thus the implementation is tolerant to arbitrary
// clock skew, but is not tolerant to arbitrary clock skew rate.
//
// However the level of tolerance to skew rate can be configured by setting
// RenewDeadline and LeaseDuration appropriately. The tolerance expressed as a
// maximum tolerated ratio of time passed on the fastest node to time passed on
// the slowest node can be approximately achieved with a configuration that sets
// the same ratio of LeaseDuration to RenewDeadline. For example if a user wanted
// to tolerate some nodes progressing forward in time twice as fast as other nodes,
// the user could set LeaseDuration to 60 seconds and RenewDeadline to 30 seconds.
//
// While not required, some method of clock synchronization between nodes in the
// cluster is highly recommended. It's important to keep in mind when configuring
// this client that the tolerance to skew rate varies inversely to master
// availability.
//
// Larger clusters often have a more lenient SLA for API latency. This should be
// taken into account when configuring the client. The rate of leader transitions
// should be monitored and RetryPeriod and LeaseDuration should be increased
// until the rate is stable and acceptably low. It's important to keep in mind
// when configuring this client that the tolerance to API latency varies inversely
// to master availability.
//
// DISCLAIMER: this is an alpha API. This library will likely change significantly
// or even be removed entirely in subsequent releases. Depend on this API at
// your own risk.
package leaderelection

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/clock"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	rl "k8s.io/client-go/tools/leaderelection/resourcelock"

	"k8s.io/klog"
)

const (
	JitterFactor = 1.2
)

// 基于选主配置，实现一个选举者
// NewLeaderElector creates a LeaderElector from a LeaderElectionConfig
func NewLeaderElector(lec LeaderElectionConfig) (*LeaderElector, error) {
	if lec.LeaseDuration <= lec.RenewDeadline {
		return nil, fmt.Errorf("leaseDuration must be greater than renewDeadline")
	}
	if lec.RenewDeadline <= time.Duration(JitterFactor*float64(lec.RetryPeriod)) {
		return nil, fmt.Errorf("renewDeadline must be greater than retryPeriod*JitterFactor")
	}
	if lec.LeaseDuration < 1 {
		return nil, fmt.Errorf("leaseDuration must be greater than zero")
	}
	if lec.RenewDeadline < 1 {
		return nil, fmt.Errorf("renewDeadline must be greater than zero")
	}
	if lec.RetryPeriod < 1 {
		return nil, fmt.Errorf("retryPeriod must be greater than zero")
	}
	if lec.Callbacks.OnStartedLeading == nil {
		return nil, fmt.Errorf("OnStartedLeading callback must not be nil")
	}
	if lec.Callbacks.OnStoppedLeading == nil {
		return nil, fmt.Errorf("OnStoppedLeading callback must not be nil")
	}

	if lec.Lock == nil {
		return nil, fmt.Errorf("Lock must not be nil.")
	}

	// 构造一个选举者
	le := LeaderElector{
		config:  lec,
		clock:   clock.RealClock{},
		metrics: globalMetricsFactory.newLeaderMetrics(),
	}
	le.metrics.leaderOff(le.config.Name)
	return &le, nil
}

// 选举配置
type LeaderElectionConfig struct {
	// 资源锁的实现对象，包含不同实现，但都实现了resourcelock.Interface 锁接口，也携带了选举者身份
	// Lock is the resource that will be used for locking
	Lock rl.Interface

	// 租约过期时长，非leader 在获取锁之前需要检查 leader 过期的时间
	// LeaseDuration is the duration that non-leader candidates will
	// wait to force acquire leadership. This is measured against time of
	// last observed ack.
	//
	// A client needs to wait a full LeaseDuration without observing a change to
	// the record before it can attempt to take over. When all clients are
	// shutdown and a new set of clients are started with different names against
	// the same leader record, they must wait the full LeaseDuration before
	// attempting to acquire the lease. Thus LeaseDuration should be as short as
	// possible (within your tolerance for clock skew rate) to avoid a possible
	// long waits in the scenario.
	//
	// Core clients default this value to 15 seconds.
	LeaseDuration time.Duration

	// 当前 leader 尝试更新锁状态的期限。
	// RenewDeadline is the duration that the acting master will retry
	// refreshing leadership before giving up.
	//
	// Core clients default this value to 10 seconds.
	RenewDeadline time.Duration

	// 抢锁时尝试间隔
	// RetryPeriod is the duration the LeaderElector clients should wait
	// between tries of actions.
	//
	// Core clients default this value to 2 seconds.
	RetryPeriod time.Duration

	// 锁状态发生变化的时候，需要进行处理的一组回调函数，包含成功回调和失败回调
	// Callbacks are callbacks that are triggered during certain lifecycle
	// events of the LeaderElector
	Callbacks LeaderCallbacks

	// 看门狗
	// WatchDog is the associated health checker
	// WatchDog may be null if its not needed/configured.
	WatchDog *HealthzAdaptor

	// ReleaseOnCancel should be set true if the lock should be released
	// when the run context is cancelled. If you set this to true, you must
	// ensure all code guarded by this lease has successfully completed
	// prior to cancelling the context, or you may have two processes
	// simultaneously acting on the critical path.
	ReleaseOnCancel bool

	// Name is the name of the resource lock for debugging
	Name string
}

// 选主回调接口
// LeaderCallbacks are callbacks that are triggered during certain
// lifecycle events of the LeaderElector. These are invoked asynchronously.
//
// possible future callbacks:
//  * OnChallenge()
type LeaderCallbacks struct {
	// 成为Leader的回调
	// OnStartedLeading is called when a LeaderElector client starts leading
	OnStartedLeading func(context.Context)
	// 失去Leader的回调
	// OnStoppedLeading is called when a LeaderElector client stops leading
	OnStoppedLeading func()
	//其它观察者，发现有新leader的回调
	// OnNewLeader is called when the client observes a leader that is
	// not the previously observed leader. This includes the first observed
	// leader when the client starts.
	OnNewLeader func(identity string)
}

// 代表一个选举者
// LeaderElector is a leader election client.
type LeaderElector struct {
	// 携带了锁实现对象，包括选举者身份，即重试策略和回调策略
	config LeaderElectionConfig

	// 最后一次获取锁记录及字节流；租约缓存
	// internal bookkeeping
	observedRecord    rl.LeaderElectionRecord
	observedRawRecord []byte

	// 表示最后一次查询时间即观察时间；观察到租约缓存时的时间戳，用以判断租约是否到期
	observedTime time.Time
	// 记录最新一个Leader，每次尝试选举后都会比较跟最新获取observedRecord的leader标识是否不同，不同则触发对应回调
	// used to implement OnNewLeader(), may lag slightly from the
	// value observedRecord.HolderIdentity if the transition has
	// not yet been reported.
	reportedLeader string

	// clock is wrapper around time to allow for less flaky testing
	clock clock.Clock

	metrics leaderMetricsAdapter

	// name is the name of the resource lock for debugging
	name string
}

// Run starts the leader election loop
func (le *LeaderElector) Run(ctx context.Context) {
	// 出现异常，则调用停止选举回调
	defer func() {
		runtime.HandleCrash()
		le.config.Callbacks.OnStoppedLeading()
	}()
	// 先去抢锁，阻塞操作
	if !le.acquire(ctx) {
		return // ctx signalled done
	}
	ctx, cancel := context.WithCancel(ctx)

	// 已不再是 leader，关闭 stop chan，停止干活儿
	defer cancel()

	// 抢到锁后，执行主函数，就是我们前面提到的 run 函数，通过 Callbacks.OnStartedLeading 回调启动
	go le.config.Callbacks.OnStartedLeading(ctx)

	// 抢到锁后，需要定期更新，确保自己一直持有该锁
	le.renew(ctx)
}

// lec中携带了竞争锁实现，和选举者身份标识
// RunOrDie starts a client with the provided config or panics if the config
// fails to validate.
func RunOrDie(ctx context.Context, lec LeaderElectionConfig) {
	// 构造了一个选举者
	le, err := NewLeaderElector(lec)
	if err != nil {
		panic(err)
	}
	if lec.WatchDog != nil {
		// 将选举者塞入到看门狗
		lec.WatchDog.SetLeaderElection(le)
	}
	// 开始选举过程
	le.Run(ctx)
}

// GetLeader returns the identity of the last observed leader or returns the empty string if
// no leader has yet been observed.
func (le *LeaderElector) GetLeader() string {
	return le.observedRecord.HolderIdentity
}

// IsLeader returns true if the last observed leader was this client else returns false.
func (le *LeaderElector) IsLeader() bool {
	return le.observedRecord.HolderIdentity == le.config.Lock.Identity()
}

// acquire loops calling tryAcquireOrRenew and returns true immediately when tryAcquireOrRenew succeeds.
// Returns false if ctx signals done.
func (le *LeaderElector) acquire(ctx context.Context) bool {
	// 初始化context，并关联了channel，待了解
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	succeeded := false
	desc := le.config.Lock.Describe()
	klog.Infof("attempting to acquire leader lease  %v...", desc)

	// 通过 wait.JitterUntil 来定期调用 tryAcquireOrRenew 方法 来获取锁
	// 如果获取不到锁，则会以 RetryPeriod 为间隔不断尝试。如果获取到锁，就通过cancel()关闭context的方式，进而关闭context对应的channel，来通知 wait.JitterUntil 停止尝试。
	wait.JitterUntil(func() {
		// 尝试获取或续期，非阻塞方法
		succeeded = le.tryAcquireOrRenew(ctx)
		le.maybeReportTransition()
		if !succeeded {
			klog.V(4).Infof("failed to acquire lease %v", desc)
			return
		}
		le.config.Lock.RecordEvent("became leader")
		le.metrics.leaderOn(le.config.Name)
		klog.Infof("successfully acquired lease %v", desc)
		// 关闭context
		cancel()
	}, le.config.RetryPeriod, JitterFactor, true, ctx.Done())
	return succeeded
}

// 在获取锁之后才会调用，它会通过持续更新资源锁的数据，来确保继续持有已获得的锁，保持自己的 leader 状态。
// renew loops calling tryAcquireOrRenew and returns immediately when tryAcquireOrRenew fails or ctx signals done.
func (le *LeaderElector) renew(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// wait.Until 会不断的调用 wait.Poll 方法，前者是进行无限循环操作，直到 context的stop chan 被关闭，
	// wait.Poll则不断的对某一条件进行检查，以 RetryPeriod 为间隔，直到该条件返回true、error或者超时（上面的 RenewDeadline 参数构造的timeoutCtx关闭）。
	wait.Until(func() {
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, le.config.RenewDeadline)
		defer timeoutCancel()
		err := wait.PollImmediateUntil(le.config.RetryPeriod, func() (bool, error) {
			return le.tryAcquireOrRenew(timeoutCtx), nil
		}, timeoutCtx.Done())

		// 判断是否出现了 leader 的切换，进而调用 Callbacks 的 OnNewLeader 方法
		le.maybeReportTransition()
		desc := le.config.Lock.Describe()
		if err == nil {
			klog.V(5).Infof("successfully renewed lease %v", desc)
			return
		}
		le.config.Lock.RecordEvent("stopped leading")
		le.metrics.leaderOff(le.config.Name)
		klog.Infof("failed to renew lease %v: %v", desc, err)
		// 关闭context的channel
		cancel()
	}, le.config.RetryPeriod, ctx.Done())

	// if we hold the lease, give it up
	if le.config.ReleaseOnCancel {
		le.release()
	}
}

// release attempts to release the leader lease if we have acquired it.
func (le *LeaderElector) release() bool {
	if !le.IsLeader() {
		return true
	}
	leaderElectionRecord := rl.LeaderElectionRecord{
		LeaderTransitions: le.observedRecord.LeaderTransitions,
	}
	if err := le.config.Lock.Update(context.TODO(), leaderElectionRecord); err != nil {
		klog.Errorf("Failed to release lock: %v", err)
		return false
	}
	le.observedRecord = leaderElectionRecord
	le.observedTime = le.clock.Now()
	return true
}

/**
renew有两个功能，获取锁，或者在已经获取锁的时候，对锁进行更新，确保锁不被他人抢走。
获取到锁，返回 true
没有获取到锁，返回 false
超时，返回 ErrWaitTimeout（errors.New(“timed out waiting for the condition”)）
*/
// tryAcquireOrRenew tries to acquire a leader lease if it is not already acquired,
// else it tries to renew the lease if it has already been acquired. Returns true
// on success else returns false.
func (le *LeaderElector) tryAcquireOrRenew(ctx context.Context) bool {
	now := metav1.Now()

	// 这个 leaderElectionRecord 就是保存在 Endpoint 的 annotation 中的值。
	// 每个节点都将 HolderIdentity 设置为自己，以及关于获取和更新锁的时间。后面会对时间进行修正，才会更新到 API server
	leaderElectionRecord := rl.LeaderElectionRecord{
		HolderIdentity:       le.config.Lock.Identity(),
		LeaseDurationSeconds: int(le.config.LeaseDuration / time.Second),
		RenewTime:            now,
		AcquireTime:          now,
	}

	// 1. 获取或者创建 ElectionRecord
	// 1. obtain or create the ElectionRecord
	oldLeaderElectionRecord, oldLeaderElectionRawRecord, err := le.config.Lock.Get(ctx)
	// 获取记录出错，有可能是记录不存在，这种错误需要处理。
	if err != nil {
		if !errors.IsNotFound(err) {
			klog.Errorf("error retrieving resource lock %v: %v", le.config.Lock.Describe(), err)
			return false
		}
		// 记录不存在的话，则创建一条新的记录
		if err = le.config.Lock.Create(ctx, leaderElectionRecord); err != nil {
			klog.Errorf("error initially creating leader election record: %v", err)
			return false
		}
		// 创建记录成功，同时表示获得了锁，返回true
		le.observedRecord = leaderElectionRecord
		le.observedTime = le.clock.Now()
		return true
	}

	// 2. 正常获取了锁资源的记录，检查锁持有者和更新时间。
	// 2. Record obtained, check the Identity & Time
	if !bytes.Equal(le.observedRawRecord, oldLeaderElectionRawRecord) {
		// 记录之前的锁持有者，其实有可能就是自己。
		le.observedRecord = *oldLeaderElectionRecord
		le.observedRawRecord = oldLeaderElectionRawRecord
		le.observedTime = le.clock.Now()
	}

	// 在满足以下所有的条件下，认为锁由他人持有，并且还没有过期，返回 false
	// a. 当前锁持有者的并非自己
	// b. 上一次观察时间 + 观测检查间隔大于现在时间，即距离上次观测的间隔，小于 `LeaseDuration` 的设置值。
	if len(oldLeaderElectionRecord.HolderIdentity) > 0 &&
		le.observedTime.Add(le.config.LeaseDuration).After(now.Time) &&
		!le.IsLeader() {
		klog.V(4).Infof("lock is held by %v and has not yet expired", oldLeaderElectionRecord.HolderIdentity)
		return false
	}

	// 走到这里有2种情况：当前leader是自己 或者 最后一次刷新时间已经到期。
	// 3. 更新资源的 annotation 内容。
	// 在本函数开头 leaderElectionRecord 有一些字段被设置成了默认值，这里来设置正确的值。
	// 3. We're going to try to update. The leaderElectionRecord is set to it's default
	// here. Let's correct it before updating.
	if le.IsLeader() {
		// 如果自己持有锁，则继承之前的获取时间和 leader 切换次数
		leaderElectionRecord.AcquireTime = oldLeaderElectionRecord.AcquireTime
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions
	} else {
		// 不是自己持有锁，且当前最后一次刷新时间已经到期，则说明可能发生 leader 切换，所以 LeaderTransitions + 1；然后去抢锁，更新成自己
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions + 1
	}

	// 更新锁资源对象，要么自己续期，要么就是上个leader已经过期，去抢锁；失败则还是保留之前信息
	// update the lock itself
	if err = le.config.Lock.Update(ctx, leaderElectionRecord); err != nil {
		klog.Errorf("Failed to update lock: %v", err)
		return false
	}

	//续期或抢锁成功，更新时钟
	le.observedRecord = leaderElectionRecord
	le.observedTime = le.clock.Now()
	return true
}

func (le *LeaderElector) maybeReportTransition() {
	if le.observedRecord.HolderIdentity == le.reportedLeader {
		return
	}
	le.reportedLeader = le.observedRecord.HolderIdentity
	if le.config.Callbacks.OnNewLeader != nil {
		go le.config.Callbacks.OnNewLeader(le.reportedLeader)
	}
}

// Check will determine if the current lease is expired by more than timeout.
func (le *LeaderElector) Check(maxTolerableExpiredLease time.Duration) error {
	if !le.IsLeader() {
		// Currently not concerned with the case that we are hot standby
		return nil
	}
	// If we are more than timeout seconds after the lease duration that is past the timeout
	// on the lease renew. Time to start reporting ourselves as unhealthy. We should have
	// died but conditions like deadlock can prevent this. (See #70819)
	if le.clock.Since(le.observedTime) > le.config.LeaseDuration+maxTolerableExpiredLease {
		return fmt.Errorf("failed election to renew leadership on lease %s", le.config.Name)
	}

	return nil
}
