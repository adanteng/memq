// store定义保证消息发送成功的存储方式
package memq

import "database/sql"

type Store interface {
	Persistence(qname string, msg *memqMsg) (string, error)
	Finish(key string) error
	Fail(key string) error

	// 这个方法仅在manager中的goroutine中使用，所以manager需要单独的store对象
	UnfinishedMsgs() ([]DBMessage, error)
	Recycle() error
}

type DBMessage struct {
	MessageID  int64  `json:"message_id"`
	MessageMd5 string `json:"message_md5"`

	Content    []byte `json:"content"`
	Createtime int64  `json:"createtime"`
	Updatetime int64  `json:"updatetime"`

	// default:0 succ:1
	Status int `json:"status"`

	// 记录内存中transport对应的key
	Qname string `json:"qname"`
}

type MemqExecer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}
