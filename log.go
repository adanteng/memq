package memq

import (
	"log"
	"os"
)

var logger memqLogger

var infoStd = log.New(os.Stdout, "info ", log.LstdFlags)
var errStd = log.New(os.Stderr, "err ", log.LstdFlags)

type memqLogger struct{}

func (l *memqLogger) Info(v ...interface{}) {
	infoStd.Print(v...)
}

func (l *memqLogger) Infof(format string, v ...interface{}) {
	infoStd.Printf(format, v...)
}

func (l *memqLogger) Error(v ...interface{}) {
	errStd.Print(v...)
}

func (l *memqLogger) Errorf(format string, v ...interface{}) {
	errStd.Printf(format, v...)
}
