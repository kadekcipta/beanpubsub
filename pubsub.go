package beanpubsub

import (
	"time"
)

type Subscriber interface {
	OnMessage(body []byte)
	OnSubscribe()
	OnUnsubscribe()
}

type InprocPubSub interface {
	Subscribe(Subscriber)
	Unsubscribe(Subscriber)
	Publish(uint32, time.Duration, time.Duration, []byte) error
}
