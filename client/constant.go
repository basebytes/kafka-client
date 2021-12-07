package client

import (
	"github.com/Shopify/sarama"
	"time"
)

const (
	EventTypeInitialError EventType =iota
	EventTypeTranceMessageError

	EventTypeProduceMsgSuccess
	EventTypeProduceMsgError

	EventTypeConsumeMsgSuccess
	EventTypeConsumeMsgError
	EventTypeConsumerGroupError

	BalanceStrategySticky BalanceStrategy="sticky"
	BalanceStrategyRoundRobin BalanceStrategy="round_robin"
	BalanceStrategyRange BalanceStrategy="range"

	PartitionStrategyRoundRobin PartitionStrategy="round_robin"
	PartitionStrategyHash PartitionStrategy="hash"
	PartitionStrategyManual PartitionStrategy="manual"
	PartitionStrategyRandom PartitionStrategy="random"


)
var (
	defaultKafkaVersion =sarama.V2_0_1_0
	defaultBalanceStrategy = sarama.BalanceStrategyRange
	defaultPartitionStrategy =sarama.NewRoundRobinPartitioner
	defaultChannelBufferSize =25
	defaultDialTimeoutSecond =2*time.Second

)

func DefaultConfig() *sarama.Config{
	config := sarama.NewConfig()
	config.ChannelBufferSize=defaultChannelBufferSize
	config.Version=defaultKafkaVersion

	config.Net.DialTimeout=defaultDialTimeoutSecond
	config.Net.SASL.Handshake = false
	config.Net.SASL.Mechanism=sarama.SASLTypePlaintext

	config.Producer.MaxMessageBytes = 10*1024*1024
	config.Producer.Return.Successes=true
	config.Producer.Partitioner = defaultPartitionStrategy

	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable=false
	config.Consumer.Group.Rebalance.Strategy = defaultBalanceStrategy

	return config
}




