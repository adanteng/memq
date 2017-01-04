// manager是transport上层，具有管理职能
package memq

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
)

var (
	ErrTransportNotExist error = errors.New("transport not exist")

	supervisorMu      sync.Mutex
	defaultSupervisor *supervisor
	started           bool
)

type supervisor struct {
	mu sync.Mutex

	//
	store      Store
	transports map[string]*Transport

	done chan error
}

func (m *supervisor) register(name string, tt *Transport) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.transports[name] = tt
}

func (m *supervisor) finish(key string) error {
	return m.store.Finish(key)
}

func (m *supervisor) fail(key string) error {
	return m.store.Fail(key)
}

func startSupervisor() {
	supervisorMu.Lock()
	defer supervisorMu.Unlock()

	if started {
		return
	}

	defaultSupervisor = &supervisor{
		store:      defaultStore,
		transports: make(map[string]*Transport),
		done:       make(chan error, 1),
	}

	go defaultSupervisor.startProcessUnfinishedMsgs(defaultSupervisor.done)
	go defaultSupervisor.startRecycleBin(defaultSupervisor.done)

	// log error
	go func() {
		errc := 1
		for err := range defaultSupervisor.done {
			logger.Error(err.Error())
			errc++

			// 2个goroutine
			if errc == 2 {
				return
			}
		}
	}()

	started = true
}

func (m *supervisor) transport(qname string) (*Transport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	transport, ok := m.transports[qname]
	if !ok {
		logger.Errorf("can not found transport for %s", qname)
		return nil, ErrTransportNotExist
	}
	return transport, nil
}

func (m *supervisor) startProcessUnfinishedMsgs(done chan error) {
	defer func() {
		if r := recover(); r != nil {
			done <- fmt.Errorf("panic in startProcessUnfinishedMsgs. %+v", r)
		}
	}()

	logger.Info("start process unfinished msgs")

	for {
		msgs, err := m.store.UnfinishedMsgs()
		if err != nil {
			logger.Errorf("get unfinished msgs failed. %+v", err)
			continue
		}

		for _, msg := range msgs {
			logger.Infof("retry msg %d", msg.MessageID)

			transport, err := m.transport(msg.Qname)
			if err != nil {
				logger.Errorf("get transport failed. %s %+v", msg.Qname, err)
				continue
			}

			messageID := strconv.FormatInt(msg.MessageID, 10)
			messageContent := &memqMsg{value: msg.Content, md5: msg.MessageMd5}
			if err := transport.memq.send(messageContent); err != nil {
				logger.Errorf("send failed. %v", err)
				continue
			}
			if transport.consistency {
				if err := m.finish(messageID); err != nil {
					logger.Errorf("finish failed. %s %v", messageID, err)
					continue
				}
			}
		}

		time.Sleep(1 * time.Second)
	}

	done <- errors.New("unexpected exit startProcessUnfinishedMsgs")
}

// TODO(lihao3) 当前回收机制比较简单粗暴
func (m *supervisor) startRecycleBin(done chan error) {
	defer func() {
		if r := recover(); r != nil {
			done <- fmt.Errorf("panic in startRecycleBin. %+v", r)
		}
	}()

	logger.Info("start recycle bin")

	for {
		if err := m.store.Recycle(); err != nil {
			logger.Errorf("recycle err. %+v", err)
		}

		time.Sleep(300 * time.Second)
	}

	done <- errors.New("unexpected exit startRecycleBin")
}
