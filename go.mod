module github.com/deeplike-ai/eventbus

go 1.23.0

retract (
    v0.1    // publihed with wrong module name
    v0.1.1  // published without retract
)


require github.com/confluentinc/confluent-kafka-go/v2 v2.5.3 // indirect
