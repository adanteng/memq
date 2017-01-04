package memq

import (
	"crypto/md5"
	"encoding/hex"
)

func genMd5(data []byte) string {
	h := md5.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

// TODO(lihao3) memq需要维护一个db对象，同时在做持久化操作时又需要tx对象，这种情况当前处理的不够优雅
var defaultStore Store

func SetDefaultStore(store Store) {
	defaultStore = store
}
