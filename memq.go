// 自己部门维护的业务系统与其他部门的业务系统有数据交互的情况出现时，一般都需要一个内存内部的q做一个异步的处理，然后根据性能要求起goroutine消耗这个q的数据，memq的目的就是做一个通用的抽象，无论底层是redis还是rabbitmq（rabbitmq虽然可定制性很高，但是互联网公司使用的场景一般比较单一，这里我仅给出一个比较通用的封装，见rabbitmq.go）
package memq

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	defaultSendTimeout   = 1 * time.Second
	defaultRecvLoopCount = 1
)

var ErrSendTimeout error = errors.New("send timeout")

// mq对上层transport提供抽象后的接口，无论下层具体使用的是rabbitmq还是nsq，对于msg的断言下层做
type memqMsg struct {
	md5   string // 写入log中，比较清晰
	value interface{}
}

// 这个包装，为了在memq.go中打印消息md5，方便查错
func NewMemqMsg(v interface{}) (*memqMsg, error) {
	m := &memqMsg{value: v}
	if err := m.setMd5(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *memqMsg) setMd5() error {
	b, err := json.Marshal(m.value)
	if err != nil {
		return err
	}
	m.md5 = genMd5(b)
	return nil
}

// mq维护内存队列，提供给多个producer竞争
type memqQueue chan *memqMsg

// mq任务：消息入队（内部队列），选择空闲producer，发出消息，通知上层是否有error-publish失败 or 内部bug，上层都会通过store机制，保证重发
type memq struct {
	pdrs *list.List

	// pdr计数器
	pc int

	// 在操作list时，需要线程安全的保证
	mu sync.Mutex

	/*
	   描述下下面3个与性能相关变量的关系，作为调用方怎么设置的参考
	   1 pdrc-生产者代表与3rd的长连接，连接线程安全，所以不同的goroutine可能拿到相同的连接，这个的发送速度很重要，如果发送速度慢，queue会被打满
	   2 cc-小于pdrc是没有意义的，可以认为cc是同时处于激活状态的长连接数量，如果cc<pdrc，当前并发大于cc小于pdrc，就会导致queue有堆积。所以，可以根据需要支持的并发量以及pdr的处理速度，推测cc和pdr的数量
	   3 对于cc<pdrc，是在queue有足够缓冲能力的情况下预估，并配置，cc代价很小，不需要这么搞

	   举个例子：
	   1w个消息每s，单pdr处理时间：20ms（每s处理50个消息），那么需要pdrc为1w/50==200，cc也要200个
	*/
	pdrc  int       // 生产者数量
	cc    int       // 消耗者数量
	queue memqQueue // 内存队列容量，这个大小一般没有太大的作用，高并发一下打满

	// <=0的情况认为没有timeout
	sendTimeout time.Duration

	// transport层传入的，掉用方初始化好这个方法
	createPdr func() (Producer, error)

	//
	memqStat *MemqStat

	//
	stopchan chan struct{}
}

type memqProducer struct {
	pdr  Producer
	name string
}

type MemqStat struct {
	// producer: Pdrc==ActivePdrc p不够用，出现超时
	IdlePdrc   int
	ActivePdrc int

	// consumer: Cc==ActiveCc c不够用，出现超时
	Cc       int
	ActiveCc int

	// CapQueue==LenQueue 队列满，c不够用
	CapQueue int
	LenQueue int
}

func (q *memq) stat() *MemqStat {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.memqStat.IdlePdrc = q.pdrs.Len()
	q.memqStat.Cc = q.cc
	q.memqStat.CapQueue = cap(q.queue)
	q.memqStat.LenQueue = len(q.queue)

	return q.memqStat
}

func (q *memq) init() error {
	logger.Info("init start")

	// pdr
	for i := 0; i < q.pdrc; i++ {
		if err := q.newPdr(); err != nil {
			return err
		}
		logger.Infof("pdr %d create succ", i)
	}

	// cr
	q.startRecvLoops()

	return nil
}

func (q *memq) closeMemq() error {
	close(q.stopchan)

	for {
		if q.pdrs.Len() == q.pdrc {
			break
		}

		// 程序有bug
		if q.pdrs.Len() > q.pdrc {
			logger.Errorf("pdrs.Len(%d) > pdrc(%d)", q.pdrs.Len(), q.pdrc)
			break
		}

		// 如果还有再工作的pdr，暂时等待，将来引入activepdr队列，可以直接关闭
		time.Sleep(300 * time.Microsecond)
	}

	return nil
}

func (q *memq) send(msg *memqMsg) error {
	if q.sendTimeout <= 0 {
		q.queue <- msg
		return nil
	}

	select {
	case <-time.After(q.sendTimeout):
		// 如果上层拿到大量这种错误，就需要调整producer的数量，提升消耗的速度，不考虑因为依赖的服务导致的性能瓶颈，请根据实际情况排查问题
		logger.Error("enqueue timeout. msgmd5:%s", msg.md5)
		return ErrSendTimeout
	case q.queue <- msg:
		logger.Infof("enqueue succ. msgmd5:%s", msg.md5)
	}
	return nil
}

func (q *memq) newPdr() error {
	p, err := q.createPdr()
	if err != nil {
		logger.Errorf("create pdr failed %v", err)
		return err
	}

	q.mu.Lock()
	q.pc++
	q.mu.Unlock()

	mp := &memqProducer{
		pdr:  p,
		name: fmt.Sprintf("%d-%d", q.pc, q.pdrc),
	}
	q.putPdr(mp, true)

	return nil
}

func (q *memq) getIdlePdr() (*memqProducer, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.pdrs.Len() > 0 {
		pdr := q.pdrs.Front()
		q.pdrs.Remove(pdr)

		// 引用计数
		q.memqStat.ActivePdrc++

		// 如果发现pdr数量不满足初始设置，需要补充，并且通过log查问题
		if q.memqStat.ActivePdrc+q.pdrs.Len() < q.pdrc {
			// TODO(lihao3) 监控活着的pdr，如果低于pdrc，需要补充，主要是发现潜在的bug
			logger.Errorf("pdr total lower than pdrc %d %d", q.memqStat.ActivePdrc+q.pdrs.Len(), q.pdrc)
		}

		return pdr.Value.(*memqProducer), nil
	}

	//
	return nil, errors.New("get idle pdr failed")
}

func (q *memq) putPdr(pdr *memqProducer, new bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.pdrs.PushBack(pdr)

	if !new {
		q.memqStat.ActivePdrc--
	}
}

func (q *memq) recvLoop(no int) {
	// recvLoop的panic是致命的，打印出来
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("panic %+v", r)

			q.mu.Lock()
			q.memqStat.ActiveCc--
			q.mu.Unlock()
		}
	}()

	for {
		select {
		case <-q.stopchan:
			logger.Infof("customer %d exit", no)
			return
		case msg := <-q.queue:
			mpdr, err := q.getIdlePdr()
			if err != nil {
				// 这里会丢弃掉消息，消息的可靠处理，会有goroutine和store配合保证
				logger.Errorf("get idle pdr failed %v", err)
				continue
			}

			start := time.Now()
			if err := mpdr.pdr.Send(msg.value); err != nil {
				// 出错，稍作停顿再向mq发起连接
				time.Sleep(100 * time.Microsecond)

				logger.Errorf("dequeue failed. err:%+v msg:%+v", err, msg)

				// 回收pdr资源，无论最终是否成功
				if err := mpdr.pdr.Close(); err != nil {
					logger.Errorf("close pdr failed. err:%+v", err)
				}
				mpdr = nil

				q.mu.Lock()
				q.memqStat.ActivePdrc--
				q.mu.Unlock()

				// 生成新的pdr，否则会导致amqp死锁
				if err := q.newPdr(); err != nil {
					logger.Errorf("append pdr failed. err:%+v", err)
				}
			} else {
				logger.Infof("dequeue succ. pdr:%s cr:%d time:%s msgmd5:%s", mpdr.name, no, time.Since(start).String(), msg.md5)
				q.putPdr(mpdr, false)
			}
		}
	}
}

func (q *memq) startRecvLoops() {
	cc := q.cc
	if cc <= 0 {
		cc = defaultRecvLoopCount
	}

	for i := 0; i < cc; i++ {
		q.mu.Lock()
		q.memqStat.ActiveCc++
		q.mu.Unlock()

		// TODO(lihao3) 用context管理loop的退出
		go q.recvLoop(i)

		logger.Infof("recvLoop %d start succ", i)
	}
}
