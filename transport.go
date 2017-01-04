// 仿照net/http，对app提供接口，利用memq发送消息，利用store保证可靠性，通过goroutine保证最终一致性
package memq

import "errors"

var (
	ErrUnexpectedStoreOp error = errors.New("unexpected store op")

	NilMessageID string = ""
)

type Transport struct {
	name        string // 标识transport
	memq        *memq  // 内存队列主逻辑
	consistency bool   // 是否需要最终一致性
}

// producer要调用方自己实现，memq才能工作，所以不提供DefaultTransport
func NewTransport(name string, consistency bool, createPdr func() (Producer, error), pdrc, cc, bufferSize int) *Transport {
	transport := &Transport{
		name:        name,
		memq:        newMemq(createPdr, pdrc, cc, bufferSize),
		consistency: consistency,
	}

	startSupervisor()
	defaultSupervisor.register(name, transport)

	return transport
}

type TransportStat struct {
	MemqStat *MemqStat
}

func (tt *Transport) Stat() *TransportStat {
	return &TransportStat{
		MemqStat: tt.memq.stat(),
	}
}

// 必须在tx中调用
func (tt *Transport) Save(store Store, msg interface{}) (string, error) {
	if !tt.consistency {
		return NilMessageID, ErrUnexpectedStoreOp
	}

	m, err := newMemqMsg(msg)
	if err != nil {
		return NilMessageID, err
	}

	messageID, err := store.Persistence(tt.name, m)
	if err != nil {
		// need rollback uplevel
		return NilMessageID, err
	}
	return messageID, nil
}

// 必须在tx.Commit()成功后调用
func (tt *Transport) MustSend(messageID string, msg interface{}) error {
	if !tt.consistency {
		return ErrUnexpectedStoreOp
	}

	m, err := newMemqMsg(msg)
	if err != nil {
		return err
	}

	if err := tt.memq.send(m); err == nil {
		return defaultSupervisor.finish(messageID)
	}

	// TODO(lihao3) supervisor处理这件事不太合适
	return defaultSupervisor.fail(messageID)
}

// 不保证一致性的情况
func (tt *Transport) Send(msg interface{}) error {
	if tt.consistency {
		return ErrUnexpectedStoreOp
	}

	m, err := newMemqMsg(msg)
	if err != nil {
		return err
	}

	return tt.memq.send(m)
}

func (tt *Transport) Close() error {
	return tt.memq.closeMemq()
}
