// 仿照net/http，对app提供接口，利用memq发送消息，利用store保证可靠性，通过goroutine保证最终一致性
package memq

import (
	"container/list"
	"errors"
	"time"
)

type Transport struct {
	manager *Manager

	// 标识transport
	qname string

	// 内存队列主逻辑
	memq memq

	// 是否需要最终一致性
	consistency bool
}

// FIXME(lihao3) 涉及到性能的参数提出了一个struct，这样增加调用方使用的复杂度
type TransportPerformance struct {
	Pdrc     int
	Cc       int
	QueueCap int
}

// producer要调用方自己实现，memq才能工作，所以不提供DefaultTransport
func NewTransport(manager *Manager, pdrFactory ProducerFactory, consistency bool, pconfig *TransportPerformance) *Transport {
	transport := &Transport{qname: pdrFactory.GetName()}

	manager.setTransport(pdrFactory.GetName(), transport)
	transport.manager = manager

	transport.memq = memq{
		pdrs:        list.New(),
		pdrc:        pconfig.Pdrc,
		queue:       make(chan *memqMsg, pconfig.QueueCap),
		sendTimeout: 1 * time.Second,
		createPdr:   pdrFactory.Create,
		cc:          pconfig.Cc,
		memqStat:    &MemqStat{},
	}
	transport.memq.init()

	transport.consistency = consistency

	return transport
}

type TransportStat struct {
	MemqStat *MemqStat
}

func (tt *Transport) stat() *TransportStat {
	return &TransportStat{
		MemqStat: tt.memq.stat(),
	}
}

func (tt *Transport) SetSendTimeout(v time.Duration) {
	tt.memq.mu.Lock()
	defer tt.memq.mu.Unlock()

	// 每次send都会有超时设置
	tt.memq.sendTimeout = v
}

var ErrUnexpectedStoreOp error = errors.New("unexpected store op")

// 必须在tx中调用
func (tt *Transport) Persistence(store Store, msg interface{}) (string, error) {
	if tt.consistency {
		m, err := NewMemqMsg(msg)
		if err != nil {
			return "", err
		}

		key, err := store.Persistence(tt.qname, m)
		if err != nil {
			// 如果这里失败，上层需要重新发送消息
			return "", err
		}
		return key, nil
	}
	return "", ErrUnexpectedStoreOp
}

// 必须在tx.Commit()成功后调用
func (tt *Transport) Send(msg interface{}) error {
	m, err := NewMemqMsg(msg)
	if err != nil {
		return err
	}

	return tt.memq.send(m)
}

// 内部监控任务是从db中查出msg内容，不需要在初始化memqMsg
func (tt *Transport) sendMemqMsg(msg *memqMsg) error {
	return tt.memq.send(msg)
}

// 必须在tx.Commit()成功后调用
func (tt *Transport) ConsistencySend(store Store, key string, msg interface{}) error {
	if !tt.consistency {
		return ErrUnexpectedStoreOp
	}

	m, err := NewMemqMsg(msg)
	if err != nil {
		return err
	}

	if err := tt.memq.send(m); err != nil {
		if serr := store.Fail(key); serr != nil {
			logger.Errorf("fail op err. %s %v", key, err)
			return serr
		}
		return err
	}

	return nil
}

// 这个允许失败，失败会导致补发
func (tt *Transport) Finish(key string) error {
	if tt.consistency {
		// 使用manager的db对象
		if err := tt.manager.store.Finish(key); err != nil {
			// 如果这里失败，消息可能发送成功，这里要求上层重发
			return err
		}
		return nil
	}
	return ErrUnexpectedStoreOp
}

func (tt *Transport) Close() error {
	return tt.memq.closeMemq()
}
