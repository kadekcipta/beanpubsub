package main

import (
	"fmt"
	"io"
	"log"
	"runtime"
	"time"

	pubsub "github.com/kadekcipta/beanpubsub"
	"golang.org/x/net/context"
)

type subscriber struct {
	name string
}

func (s *subscriber) OnMessage(data []byte) {
	fmt.Println(s.name, string(data))
}

func (s *subscriber) OnSubscribe() {
	fmt.Println("Subscribe ", s.name)
}

func (s *subscriber) OnUnsubscribe() {
	fmt.Println("Unsubscribe ", s.name)
}

func (s *subscriber) String() string {
	return s.name
}

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())

	c, cancel := context.WithCancel(context.Background())

	m, err := pubsub.New(c, "localhost:11300", "hello")
	if err != nil {
		panic(err)
	}

	subs := []pubsub.Subscriber{}

	for i := 0; i < 2000; i++ {
		subs = append(subs, &subscriber{fmt.Sprintf("Subscriber #%d", i+1)})
		m.Subscribe(subs[i])
	}

	// next seconds later, subscriber1 unsubscribe
	go func() {
		<-time.After(10 * time.Second)
		for i := 10; i < 2000; i++ {
			m.Unsubscribe(subs[i])
		}

		<-time.After(10 * time.Second)
		for i := 11; i < 1000; i++ {
			m.Subscribe(&subscriber{fmt.Sprintf("Subscriber #%d", i)})
		}
	}()

	go func() {
		for {
			err := m.Publish(
				1,
				0,
				time.Second*1,
				[]byte("hello world !"),
			)
			if err != nil {
				log.Println(err)
			}

			<-time.After(1 * time.Second)
		}
	}()

	fmt.Println("press enter to exit")
	fmt.Scanln()
	cancel()
	m.(io.Closer).Close()
}
