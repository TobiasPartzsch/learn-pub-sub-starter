package pubsub

type SimpleQueueType int

const (
	Durable   SimpleQueueType = iota // 0
	Transient                        // 1
)
