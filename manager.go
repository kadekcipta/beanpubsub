package pubsub

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/kr/beanstalk"
	"golang.org/x/net/context"
)

const (
	MaxQueueSize = 50         // watermark before messages overflowing
	DefaultTopic = "_pubsub_" // default tubeset name that represent default topic
)

var (
	ErrNotConnected   = errors.New("pubsub: not connected to beanstalkd")
	ErrMessageDropped = errors.New("pubsub: message dropped")
)

type subscriptionQueue struct {
	queue      chan []byte
	subscriber Subscriber
	c          context.Context
}

func (s *subscriptionQueue) close() {
	close(s.queue)
}

func (s *subscriptionQueue) put(data []byte) error {
	select {
	case s.queue <- data:
		return nil
	default:
		// message overflow, discard it
		return ErrMessageDropped
	}
}

func (s *subscriptionQueue) waitMessages() {
	for m := range s.queue {
		s.subscriber.OnMessage(m)
	}
}

type beanstalkdPubSub struct {
	sync.RWMutex
	subscribers []*subscriptionQueue
	connSub     *beanstalk.Conn
	connPub     *beanstalk.Conn
	topic       string
	tube        *beanstalk.Tube
	address     string
	c           context.Context
}

// Close closes all subscribers
func (m *beanstalkdPubSub) Close() error {
	for _, item := range m.subscribers {
		item.close()
	}

	// store last found error
	var err error

	if m.connSub != nil {
		err = m.connSub.Close()
	}

	if m.connPub != nil {
		if e := m.connPub.Close(); e != nil {
			// update error
			err = e
		}
	}

	return err
}

// Subscribe puts subscriber into the topic's subscription list
func (m *beanstalkdPubSub) Subscribe(s Subscriber) {
	m.Lock()
	defer m.Unlock()

	// wrap it in subscriber queue
	sq := &subscriptionQueue{
		queue:      make(chan []byte, MaxQueueSize),
		subscriber: s,
	}

	m.subscribers = append(m.subscribers, sq)
	sq.subscriber.OnSubscribe()
	// start polling
	go sq.waitMessages()
}

// Unsubscribe remove subscriber from topic's subscription list
func (m *beanstalkdPubSub) Unsubscribe(s Subscriber) {
	m.Lock()
	defer m.Unlock()

	for i, sq := range m.subscribers {
		if s == sq.subscriber {
			sq.subscriber.OnUnsubscribe()
			sq.close()

			subscribers := m.subscribers
			subscribers, subscribers[len(subscribers)-1] = append(subscribers[:i], subscribers[i+1:]...), nil
			m.subscribers = subscribers
			break
		}
	}
}

// broadcast puts the message in the subscriber's internal queue
// a goroutine will pop the message off the queue and call the subscriber's OnMessage
func (m *beanstalkdPubSub) broadcast(data []byte) {
	m.Lock()
	defer m.Unlock()

	for _, sq := range m.subscribers {
		sq.put(data)
	}
}

// Publish puts the message for specified topic to a beanstalkd tube
func (m *beanstalkdPubSub) Publish(priority uint32, delay, ttr time.Duration, data []byte) error {

	m.Lock()
	defer m.Unlock()

	// send to beanstalkd to certain tube
	if m.connSub == nil {
		return ErrNotConnected
	}

	if m.tube == nil {
		m.tube = &beanstalk.Tube{m.connPub, m.topic}
	}

	_, err := m.tube.Put(data, priority, delay, ttr)

	if err != nil {
		return err
	}

	return nil
}

func (m *beanstalkdPubSub) watchIncomingMessages() {
	if m.connSub == nil {
		return
	}

	// create tubeset for topic
	tubeset := beanstalk.NewTubeSet(m.connSub, m.topic)

	for {
		select {
		// watch for close signal
		case <-m.c.Done():
			return
		default:
			// get the message
			id, body, err := tubeset.Reserve(time.Minute)
			if err == nil {
				// broadcast it to all subscribers
				m.broadcast(body)
				// simply delete the message
				m.connSub.Delete(id)
				continue
			}

			if err.(beanstalk.ConnError).Err == beanstalk.ErrTimeout {
				// re-reserve
				continue
			} else if err.(beanstalk.ConnError).Err == beanstalk.ErrDeadline {
				time.Sleep(time.Second)
				// re-reserve
				continue
			}
		}
	}
}

func (m *beanstalkdPubSub) ensureConnect() error {
	// open connection for subscription
	cr, err := beanstalk.Dial("tcp", m.address)
	if err != nil {
		return err
	}

	m.connSub = cr

	// open connection for publishing
	cw, err := beanstalk.Dial("tcp", m.address)
	if err != nil {
		return err
	}

	m.connPub = cw

	return nil
}

func New(c context.Context, address, topic string) (InprocPubSub, error) {
	m := &beanstalkdPubSub{
		address:     address,
		topic:       topic,
		subscribers: []*subscriptionQueue{},
		c:           c,
	}

	// verify topic name and make sure it's not empty string
	if strings.TrimSpace(topic) == "" {
		m.topic = DefaultTopic
	}

	if err := m.ensureConnect(); err != nil {
		return nil, err
	}

	go m.watchIncomingMessages()

	return m, nil
}
