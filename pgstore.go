package memq

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// 每次数据库事务操作需要用新的store对象
// FIXME(lihao3) 对象中包含interface和db，这样设计，上层在初始化store时，很容易用错
type PGStore struct {
	// tx or db
	execer MemqExecer

	// db
	db *sql.DB
}

// FIXME(lihao3) 优化初始化对象的方式，区分tx和db
func NewPGStore(execer MemqExecer, db *sql.DB) *PGStore {
	return &PGStore{
		execer: execer,

		db: db,
	}
}

// maybe tx
// 允许重发的情况出现，这里限制任何消息的状态
func (s *PGStore) Persistence(qname string, msg *memqMsg) (string, error) {
	b, err := json.Marshal(msg.value)
	if err != nil {
		return "", err
	}

	messageID, err := s.get(msg.md5)
	if err != nil {
		if err == sql.ErrNoRows {
			return s.add(qname, b, msg.md5)
		} else {
			return "", err
		}
	}

	return messageID, s.reset(messageID)
}

func (s *PGStore) get(messageMd5 string) (string, error) {
	sql := "select message_id from plsv2.pls_message where message_md5=$1"

	var id int64
	if err := s.execer.QueryRow(sql, messageMd5).Scan(&id); err != nil {
		return "", err
	}
	return strconv.FormatInt(id, 10), nil
}

func (s *PGStore) add(qname string, b []byte, messageMd5 string) (string, error) {
	sql := "insert into plsv2.pls_message (message_md5, content, createtime, updatetime, qname) values($1, $2, $3, $4, $5) returning message_id"

	binds := make([]interface{}, 5)
	binds[0] = messageMd5
	binds[1] = b
	binds[2] = time.Now().Unix()
	binds[3] = time.Now().Unix()
	binds[4] = qname

	var id int64
	if err := s.execer.QueryRow(sql, binds...).Scan(&id); err != nil {
		return "", err
	}

	return strconv.FormatInt(id, 10), nil
}

func (s *PGStore) reset(messageID string) error {
	sql := "update plsv2.pls_message set status=0 where message_id=$1"
	_, err := s.execer.Exec(sql, messageID)
	return err
}

// must not tx
func (s *PGStore) Finish(messageID string) error {
	if _, err := s.db.Exec("update plsv2.pls_message set status=1, updatetime=$1 where message_id=$2", time.Now().Unix(), messageID); err != nil {
		logger.Errorf("Finish err. %s %s", messageID, err.Error())
		return err
	}
	return nil
}

// must not tx
func (s *PGStore) Fail(messageID string) error {
	if _, err := s.db.Exec("update plsv2.pls_message set status=2, updatetime=$1 where message_id=$2 and status=0", time.Now().Unix(), messageID); err != nil {
		logger.Errorf("Fail err. %s %s", messageID, err.Error())
		return err
	}
	return nil
}

// must not tx
func (s *PGStore) UnfinishedMsgs() ([]DBMessage, error) {
	var r []DBMessage

	f, err := s.failedMsgs()
	if err != nil {
		return nil, err
	}

	e, err := s.expiredMsgs()
	if err != nil {
		return nil, err
	}

	r = append(r, f...)
	r = append(r, e...)

	return r, nil
}

func (s *PGStore) failedMsgs() ([]DBMessage, error) {
	return s.query("select message_id, message_md5, qname, content from plsv2.pls_message where status=2 limit 50")
}

// TODO(lihao3) 30分钟没有被处理的订单，需要重新发送，大量订单涌入pg，c过少会导致超时（不一定失败），这种重发没有问题
func (s *PGStore) expiredMsgs() ([]DBMessage, error) {
	sql := fmt.Sprintf("select message_id, message_md5, qname, content from plsv2.pls_message where status=0 and createtime<%d", time.Now().Unix()-30*60)
	return s.query(sql)
}

func (s *PGStore) query(sql string) ([]DBMessage, error) {
	rows, err := s.db.Query(sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []DBMessage
	for rows.Next() {
		var messageID int64
		var messageMd5 string
		var qname string
		var content []byte
		if err := rows.Scan(&messageID, &messageMd5, &qname, &content); err != nil {
			return nil, err
		}
		msgs = append(msgs, DBMessage{MessageID: messageID, MessageMd5: messageMd5, Qname: qname, Content: content})
	}
	return msgs, nil
}

func (s *PGStore) Recycle() error {
	if _, err := s.db.Exec("delete from plsv2.pls_message where status=1 and createtime<$1", time.Now().Unix()-7*86400); err != nil {
		return err
	}
	return nil
}
