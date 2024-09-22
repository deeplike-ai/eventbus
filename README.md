# Go Eventbus

Eventbus is a Go library that provides a simplified interface to handle Kafka Topics

## Motivation

to ensure high availability of software services built on the microservice architecture it is crucial for each microservice to not depend on each other.
One possible solution to decouple microservices is to use a message broker. While there are quite a lot of convenient Go libraries for REST based communication there are less libraries providing a convenient way to handle messaging. Eventbus tries to fill this cap and provides a convenient way to handle Kafka based messages.

## Getting Started

First ensure that you have installed all dependencies for [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)

get the eventbus library

```bash
go get github.com/deeplike-ai/eventbus
```

## Example

Create EventBus

```golang
import (
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/deeplike-ai/eventbus"
)

func main() {
    consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers": broker,
        "group.id":          groupId,
        "auto.offset.reset": "earliest",
    })

    eb := eventbus.NewEventBus(consumer, time.Second, 2*time.Second)
}
```

Create Handler for incoming events

```golang
func eventHandler(event *eventbus.Event) error {
    msg := string(event.Message)
    log.Printf("Topic %v@%v: %v", event.Topic, event.Offset, msg)

    return nil
}
```

Subscribe to topic

```golang
err := eb.Subscribe("Test-Topic", eventHandler, 1)
err != nil {
    panic(err)
}
```

Start listening for new events

```golang
go func() {
    log.Println("starting eventbus...")
    eb.StartAndListen()
}()
```

Close eventbus for gracefully shutdown

```golang
eb.Close()
```

## Limitations

- This library is not a full kafka client and depends therefore on the [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) library as the low level kafka client.
- messages are not committed from the eventbus directly you must commit it on your own or the kafka consumer must set enable.auto.commit to true (default)
- only retrieving messages from topics

## Feedback

If you find bugs or have any suggestion feel free to create an issue.
