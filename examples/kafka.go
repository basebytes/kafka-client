package examples

import (
	"github.com/Shopify/sarama"
	"github.com/basebytes/kafka-client/client"
	"log"
	"strings"
)

var producerRequires = []string{"servers", "producerTopic"}
var consumerRequires = []string{"servers", "consumerTopic", "groupId"}

const (
	consumer = iota
	producer
)

func TranceProducerConfig(configs *client.Config) (string, []string, *sarama.Config, error) {
	_, topic, addr, cfg, err := tranceConfig(configs, producerRequires, producer)
	return topic, addr, cfg, err
}

func TranceConsumerConfig(configs *client.Config) (string, string, []string, *sarama.Config, error) {
	return tranceConfig(configs, consumerRequires, consumer)
}

func ConsumerLogHook(eventType client.EventType, value interface{}, err error) {
	switch eventType {
	case client.EventTypeConsumerGroupError:
		log.Printf("Consumer error: %v", err)
	case client.EventTypeConsumeMsgSuccess:
		msg := value.(*sarama.ConsumerMessage)
		log.Printf("Message on %s[%d]@%d: %s", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
	case client.EventTypeConsumeMsgError:
		msg := value.(*sarama.ConsumerMessage)
		log.Printf("Consumer error on %s[%d]@%d: %s,error: %s ", msg.Topic, msg.Partition, msg.Offset, string(msg.Value), err.Error())
	}
}

func ProducerLogHook(eventType client.EventType, value interface{}, err error) {
	switch eventType {
	case client.EventTypeTranceMessageError:
		log.Printf("Producer error: %v (%v)", err, value)
	case client.EventTypeProduceMsgSuccess:
		msg := value.(*sarama.ProducerMessage)
		v, _ := msg.Value.Encode()
		log.Printf("Delivered message to %s[%d]@%d(%s)", msg.Topic, msg.Partition, msg.Offset, v)
	case client.EventTypeProduceMsgError:
		msg := value.(*sarama.ProducerError)
		log.Printf("Delivery failed: %s[%d]@%d:error %s", msg.Msg.Topic, msg.Msg.Partition, msg.Msg.Offset, msg.Unwrap())
	}
}

func tranceConfig(configs *client.Config, requires []string, flag int) (string, string, []string, *sarama.Config, error) {
	if err := configs.CheckParams(nil, requires); err != nil {
		return "", "", nil, nil, err
	}
	groupId, topic, addr, kafkaConfig := "", "", make([]string, 0), client.DefaultConfig()
	for k, v := range configs.Params {
		switch k {
		case "producerTopic", "consumerTopic":
			if (k == "producerTopic" && flag == producer) || (k == "consumerTopic" && flag == consumer) {
				topic = v
			}
		case "servers":
			addr = strings.Split(v, ",")
		case "groupId":
			groupId = v
		case "autoOffsetReset":
			if v == "latest" {
				kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
			}
		case "enableAutoCommit":
			if v != "false" {
				kafkaConfig.Consumer.Offsets.AutoCommit.Enable = true
			}
		}
	}
	return groupId, topic, addr, kafkaConfig, nil
}
