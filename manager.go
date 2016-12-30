// manager是transport上层，具有管理职能
package memq

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

var (
	defaultManager *Manager
)

func init() {
	defaultManager = &Manager{}
}

type Manager struct {
	mu sync.Mutex

	// 全局的store，各transport不能有自己的store
	store      Store
	transports map[string]*Transport

	// goroutine只启动一次
	unfinishedMsgProcessorStarted bool
	recycleBinStarted             bool
}

func NewManager(store Store) *Manager {
	defaultManager.store = store
	go defaultManager.startProcessUnfinishedMsgs()
	go defaultManager.startRecycleBin()

	return defaultManager
}

type ManagerStat struct {
	TransportStats map[string]*TransportStat

	UnfinishedMsgProcessorStarted bool
	RecycleBinStarted             bool
}

func (m *Manager) Stat() *ManagerStat {
	m.mu.Lock()
	defer m.mu.Unlock()

	stat := ManagerStat{
		UnfinishedMsgProcessorStarted: m.unfinishedMsgProcessorStarted,
		RecycleBinStarted:             m.recycleBinStarted,
		TransportStats:                make(map[string]*TransportStat, len(m.transports)),
	}

	for _, tt := range m.transports {
		stat.TransportStats[tt.qname] = tt.stat()
	}

	return &stat
}

func (m *Manager) getTransport(qname string) (*Transport, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	transport, ok := m.transports[qname]
	if !ok {
		return nil, fmt.Errorf("can not found transport for %s", qname)
	}
	return transport, nil
}
func (m *Manager) setTransport(name string, tt *Transport) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// lazy load
	if m.transports == nil {
		m.transports = make(map[string]*Transport)
	}

	m.transports[name] = tt
}

// TODO(lihao3) 增加对该goroutine的监控，退出管理
func (m *Manager) startProcessUnfinishedMsgs() {
	if r := recover(); r != nil {
		logger.Errorf("panic in startProcessUnfinishedMsgs. %v", r)

		m.mu.Lock()
		m.unfinishedMsgProcessorStarted = false
		m.mu.Unlock()
	}

	// 防止goroutine启多个
	m.mu.Lock()
	if m.unfinishedMsgProcessorStarted {
		m.mu.Unlock()
		return
	}
	m.unfinishedMsgProcessorStarted = true
	m.mu.Unlock()

	logger.Info("start process unfinished msgs.")

	for {
		msgs, err := m.store.UnfinishedMsgs()
		if err != nil {
			logger.Errorf("get unfinished msgs failed. %+v", err)
			continue
		}

		for _, msg := range msgs {
			logger.Infof("retry msg %d", msg.MessageID)

			transport, err := m.getTransport(msg.Qname)
			if err != nil {
				logger.Errorf("get transport failed. %s %+v", msg.Qname, err)
				continue
			}
			if err := transport.sendMemqMsg(&memqMsg{value: msg.Content, md5: msg.MessageMd5}); err != nil {
				logger.Errorf("send failed. %v", err)
				continue
			}
			if err := transport.Finish(strconv.FormatInt(msg.MessageID, 10)); err != nil {
				logger.Errorf("finish failed. %v", err)
				continue
			}
		}

		time.Sleep(1 * time.Second)
	}
}

// TODO(lihao3) 当前回收机制比较简单粗暴
func (m *Manager) startRecycleBin() {
	if r := recover(); r != nil {
		logger.Errorf("panic in startRecycleBin. %v", r)

		m.mu.Lock()
		m.recycleBinStarted = false
		m.mu.Unlock()
	}

	// 防止goroutine启多个
	m.mu.Lock()
	if m.recycleBinStarted {
		m.mu.Unlock()
		return
	}
	m.recycleBinStarted = true
	m.mu.Unlock()

	logger.Info("start recycle bin.")

	for {
		if err := m.store.Recycle(); err != nil {
			logger.Errorf("recycle err. %v", err)
		}

		time.Sleep(300 * time.Second)
	}

}
