package memq

import (
	"database/sql"
	"time"
)

type PGGlobalLock struct {
	db *sql.DB

	tx *sql.Tx
}

func (l *PGGlobalLock) Lock() error {
	// 不关心是否插入成功，可以认为总有插入成功的，大家竞争那一条就行了
	l.insertLock()

	tx, err := l.db.Begin()
	if err != nil {
		logger.Errorf("begin tx failed %+v", err)
		return err
	}
	l.tx = tx

	var id int64
	if err := l.tx.QueryRow("select lock_id from plsv2.pls_message_lock where lock_id=1 for update").Scan(&id); err != nil {
		logger.Errorf("lock failed err %+v", err)
		return err
	}

	return nil
}

func (l *PGGlobalLock) Unlock() error {
	if err := l.tx.Commit(); err != nil {
		logger.Errorf("unlock failed err %+v", err)
		return err
	}
	return nil
}

func (l *PGGlobalLock) insertLock() error {
	sql := "insert into plsv2.pls_message_lock (lock_id, lock_name, lock_status, updatetime) values(1, $1, $2, $3)"

	binds := make([]interface{}, 3)
	binds[0] = "global_lock"
	binds[1] = 0
	binds[2] = time.Now().Unix()

	if _, err := l.db.Exec(sql, binds...); err != nil {
		logger.Infof("insertLock failed. May be already exist. err %+v", err)
		return err
	}

	return nil
}

func SetPGGlobalLock(tdb *sql.DB) {
	defaultGlobalLock = &PGGlobalLock{db: tdb}
}
