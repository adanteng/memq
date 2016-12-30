package memq

type Producer interface {
	Send(msg interface{}) error
	Close() error
}

type ProducerFactory interface {
	Create() (Producer, error)

	GetName() string
}
