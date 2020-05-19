/*
Copyright 2016 The Kubernetes Authors.

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

package resourcelock

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	coordinationv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	LeaderElectionRecordAnnotationKey = "control-plane.alpha.kubernetes.io/leader"
	EndpointsResourceLock             = "endpoints"
	ConfigMapsResourceLock            = "configmaps"
	LeasesResourceLock                = "leases"
	EndpointsLeasesResourceLock       = "endpointsleases"
	ConfigMapsLeasesResourceLock      = "configmapsleases"
)

// leader选举记录，这个锁信息，就是存在 K8s 的 ConfigMap 或者 Endpoint 里面的，当然，存哪里可能大家已经想到了，只能存 annotation 里面，该 annotation 的 key 就是 control-plane.alpha.kubernetes.io/leader
// leader 会在 某个namespace(如kube-system下的同名 endpoint 的 annotations中写入key 值为LeaderElectionRecordAnnotationKey，value 为LeaderElectionRecord类型的记录，表明自己身份
//  同时会根据 –leader-elect-renew-deadline 参数定期去更新记录中的 RenewTime 字段（续约，合同年限为 LeaseDurationSeconds）。
// LeaderElectionRecord is the record that is stored in the leader election annotation.
// This information should be used for observational purposes only and could be replaced
// with a random string (e.g. UUID) with only slight modification of this code.
// TODO(mikedanese): this should potentially be versioned
type LeaderElectionRecord struct {
	// HolderIdentity is the ID that owns the lease. If empty, no one owns this lease and
	// all callers may acquire. Versions of this library prior to Kubernetes 1.14 will not
	// attempt to acquire leases with empty identities and will wait for the full lease
	// interval to expire before attempting to reacquire. This value is set to empty when
	// a client voluntarily steps down.
	// leader 标识，通常为 hostname
	HolderIdentity string `json:"holderIdentity"`
	// leader合同年限，同启动参数 --leader-elect-lease-duration
	LeaseDurationSeconds int `json:"leaseDurationSeconds"`
	// Leader 第一次成功获得租约时的时间戳
	AcquireTime metav1.Time `json:"acquireTime"`
	// leader 定时 renew 的时间戳
	RenewTime metav1.Time `json:"renewTime"`

	LeaderTransitions int `json:"leaderTransitions"`
}

// EventRecorder records a change in the ResourceLock.
type EventRecorder interface {
	Eventf(obj runtime.Object, eventType, reason, message string, args ...interface{})
}

// ResourceLockConfig common data that exists across different
// resource locks
type ResourceLockConfig struct {
	// 竞争者身份
	// Identity is the unique string identifying a lease holder across
	// all participants in an election.
	Identity string
	// 事件记录
	// EventRecorder is optional.
	EventRecorder EventRecorder
}

// leader选举接口
// Interface offers a common interface for locking on arbitrary
// resources used in leader election.  The Interface is used
// to hide the details on specific implementations in order to allow
// them to change over time.  This interface is strictly for use
// by the leaderelection code.
type Interface interface {

	// 获取、创建、更新 annotations 中的选举记录；估计对大多数集群来说，只有第一次的时候才会调用 Create 创建这个对象
	// Get returns the LeaderElectionRecord
	Get(ctx context.Context) (*LeaderElectionRecord, []byte, error)

	// Create attempts to create a LeaderElectionRecord
	Create(ctx context.Context, ler LeaderElectionRecord) error

	// Update will update and existing LeaderElectionRecord
	Update(ctx context.Context, ler LeaderElectionRecord) error

	// 这个 event 属于锁资源，里面会记录 leader 切换等事件
	// RecordEvent is used to record events
	RecordEvent(string)

	// leader选主身份表示
	// Identity will return the locks Identity
	Identity() string

	// Describe is used to convert details on current resource lock
	// into a string
	Describe() string
}

// 根据指定类型，初始化不同的资源锁
// Manufacture will create a lock of a given type according to the input parameters
func New(lockType string, ns string, name string, coreClient corev1.CoreV1Interface, coordinationClient coordinationv1.CoordinationV1Interface, rlc ResourceLockConfig) (Interface, error) {
	endpointsLock := &EndpointsLock{
		EndpointsMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Client:     coreClient,
		LockConfig: rlc,
	}
	configmapLock := &ConfigMapLock{
		ConfigMapMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Client:     coreClient,
		LockConfig: rlc,
	}
	leaseLock := &LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
		},
		Client:     coordinationClient,
		LockConfig: rlc,
	}
	switch lockType {
	case EndpointsResourceLock:
		return endpointsLock, nil
	case ConfigMapsResourceLock:
		return configmapLock, nil
	case LeasesResourceLock:
		return leaseLock, nil
	case EndpointsLeasesResourceLock:
		return &MultiLock{
			Primary:   endpointsLock,
			Secondary: leaseLock,
		}, nil
	case ConfigMapsLeasesResourceLock:
		return &MultiLock{
			Primary:   configmapLock,
			Secondary: leaseLock,
		}, nil
	default:
		return nil, fmt.Errorf("Invalid lock-type %s", lockType)
	}
}
