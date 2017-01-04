package memq

type Producer interface {
	Send(msg interface{}) error
	Close() error
}
