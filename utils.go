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
