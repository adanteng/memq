// supervisor只支持整个集群一个goroutine，竞争锁作为启动supervisor的条件
// 可以通过ip划分消息，每台机器启动supervisor处理自己的消息，当出现轮空的ip时，需要有机制将各ip段分配在所有启动goroutine上
// 也可以针对message_id(bigint)划分出几个大段，每个机器领取一个大段，内部supervisor线程继续划分该段为更小的单位
// 按照当前应用场景，只需要用global lock控制supervisor启动就行
package memq

type GlobalLock interface {
	Lock() error
	Unlock() error
}

var defaultGlobalLock GlobalLock
