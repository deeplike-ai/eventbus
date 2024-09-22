/**
 * Copyright 2024 Deeplike GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eventbus

import (
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Error types
var (
	ErrEmptyHandler       = errors.New("cannot subscribe if handler is empty")
	ErrEmptyTopic         = errors.New("cannot subscribe if topic is not assigned")
	ErrInvalidQueueLength = errors.New("queue length must be bigger than 0")
)

// Event represent a new received message from Kafka consumer
type Event struct {
	Message   []byte
	Topic     string
	Offset    kafka.Offset
	Partition int32
}

// EventHandler defines a function to handle new received events
type EventHandler func(*Event) error

// EventBus is an interface defining the operation
// to control listening to the kafka consumer client
// and to subscribe to topics
type EventBus interface {

	// Subscribe will subscribe the kafka consumer client to the topic.
	//
	// handler EventHandler: will be executed, when new message are read.
	//
	// queueLength int: defines how many messages are polled
	// before the EventBus call is blocked
	Subscribe(topic string, handler EventHandler, queueLength int) error

	// StartAndListen starts to poll new messages for the configured readTimeout
	// before verifying again if the EventBus should be still active
	//
	// NOTE: this is a blocking call
	StartAndListen()

	// graceful closing kafka consumer client and closing all go chan
	Close()
}

// Creates an instance of EventBus
//
// current implementation relies on the kafka consumer to be configured with
// enable.auto.commit: true, eventbus does not commit messages
//
// to enable graceful shutdown, set readTimeout smaller than shutdownTimeout
func NewEventBus(consumer *kafka.Consumer, readTimeout time.Duration, shutdownTimeout time.Duration) EventBus {
	return &kafkaEventBus{consumer: consumer, done: make(chan bool), readTimeout: readTimeout, shutdownTimeout: shutdownTimeout}
}

// kafkaEventBus implements the EventBusConsumer interface
type kafkaEventBus struct {
	consumer         *kafka.Consumer
	eventTopicWorker sync.Map
	isListening      int32
	done             chan bool
	readTimeout      time.Duration
	shutdownTimeout  time.Duration
}

func (eb *kafkaEventBus) StartAndListen() {
	atomic.StoreInt32(&eb.isListening, 1)
	for atomic.LoadInt32(&eb.isListening) == 1 {
		msg, err := eb.consumer.ReadMessage(eb.readTimeout)
		if err != nil && err.(kafka.Error).IsTimeout() {
			continue
		}

		if err != nil {
			log.Printf("failed to read message: %v. read message again", err)
			continue
		}

		topic := *msg.TopicPartition.Topic
		offset := msg.TopicPartition.Offset

		emGeneric, ok := eb.eventTopicWorker.Load(topic)
		if !ok {
			log.Printf("{topic: %v, offset: %v}: failed to load handler\n", topic, offset)
			continue
		}

		em, ok := emGeneric.(*eventMessageWorker)
		if !ok {
			log.Printf("{topic: %v, offset: %v}: unexpected type in event message worker found %T expected %T\n", topic, offset, em, eventMessageWorker{})
			continue
		}

		em.work <- &Event{msg.Value, topic, kafka.Offset(offset), msg.TopicPartition.Partition}
	}
	eb.done <- true
}

func (eb *kafkaEventBus) Subscribe(topic string, handler EventHandler, queueLength int) error {
	if topic == "" {
		return ErrEmptyTopic
	}

	if handler == nil {
		return ErrEmptyHandler
	}

	if queueLength < 1 {
		return ErrInvalidQueueLength
	}

	err := eb.consumer.Subscribe(topic, nil)
	if err != nil {
		return err
	}

	em := eventMessageWorker{handler: handler, work: make(chan *Event, queueLength), done: make(chan bool)}
	eb.eventTopicWorker.Store(topic, &em)
	go em.StartWorking()
	return nil
}

func (eb *kafkaEventBus) Close() {
	if atomic.LoadInt32(&eb.isListening) == 1 {
		atomic.StoreInt32(&eb.isListening, 0)
		<-eb.done
	}

	eb.eventTopicWorker.Range(func(key, value any) bool {
		em, ok := value.(*eventMessageWorker)
		if !ok {
			log.Printf("failed to retrieve eventMessageWorker for topic %v", key)
			return true
		}

		close(em.work)

		select {
		case <-em.done:
		case <-time.After(eb.shutdownTimeout):
			log.Printf("timeout waiting for worker on topic %v to finish, shutting down forcefully\n", key)
		}

		close(em.done)
		return true
	})

	eb.consumer.Close()
}

type eventMessageWorker struct {
	handler EventHandler
	work    chan *Event
	done    chan bool
}

func (em *eventMessageWorker) StartWorking() {
	for event := range em.work {
		err := em.handler(event)
		if err != nil {
			log.Printf("{topic: %v, offset: %v} failed to handle message: %v\n", event.Topic, event.Offset, err)
			continue
		}
	}
	em.done <- true
}
