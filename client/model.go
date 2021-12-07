package client

import (
	"fmt"
	"github.com/Shopify/sarama"
)

type Config struct {
	Name   MessageType	`json:"name"`
	Params map[string]string	`json:"params"`
}

type ConfigSlice []*Config

func (s ConfigSlice) FindConfig(name MessageType) *Config {
	for _, cfg := range s {
		if cfg!=nil&&cfg.Name==name {
			return cfg
		}
	}
	return nil
}
//return topic, servers, kafkaConfig, error
type TranceProducerConfigFunc func (configs *Config) (string,[]string,*sarama.Config,error)
//return groupId,topic, servers, kafkaConfig, error
type TranceConsumerConfigFunc func (configs *Config) (string,string,[]string,*sarama.Config,error)

type EventHook func(EventType,interface{},error)

type Message map[string]interface{}

type EventType int

type BalanceStrategy string
type PartitionStrategy string
type MessageType string

func (c *Config) CheckParams(defaults map[string]string,requires []string) error{
	if defaults!=nil{
		if c.Params == nil {
			c.Params = map[string]string{}
			for k, v := range defaults {
				c.Params[k]=v
			}
		} else {
			for k, dv := range defaults {
				if v:= c.Get(k, dv); v == dv {
					c.SetKey(k, dv)
				}
			}
		}
	}
	if requires!=nil{
		for _,key:=range requires{
			if v := c.Get(key, ""); v == "" {
				if v == "" {
					return newMissingConfigError(key)
				}
			}

		}
	}
	return nil
}

func (c *Config) SetKey(key,value string) {
	c.Params[key]=value
}
func (c *Config) Get(key,dftValue string) string{
	if c.Params !=nil{
		if v,OK:=c.Params[key];OK{
			return v
		}
	}
	return dftValue
}

func newMissingConfigError(name string) *MissingConfigError{
	return &MissingConfigError{Name:name}
}

type MissingConfigError struct {
	Name string
}

func (m MissingConfigError) Error() string{
	return fmt.Sprintf("Undefined kafka config %s.",m.Name)
}

func ParsePartitionStrategy(strategy PartitionStrategy) sarama.PartitionerConstructor {
	var result  sarama.PartitionerConstructor
	switch strategy {
	case PartitionStrategyRoundRobin:
		result = sarama.NewRoundRobinPartitioner
	case PartitionStrategyHash:
		result = sarama.NewHashPartitioner
	case PartitionStrategyManual:
		result = sarama.NewManualPartitioner
	case PartitionStrategyRandom:
		result = sarama.NewRandomPartitioner
	default:
		result = defaultPartitionStrategy
	}
	return result
}


func ParseBalanceStrategy(strategy BalanceStrategy) sarama.BalanceStrategy{
	var result  sarama.BalanceStrategy
	switch strategy {
	case BalanceStrategySticky:
		result = sarama.BalanceStrategySticky
	case BalanceStrategyRoundRobin:
		result = sarama.BalanceStrategyRoundRobin
	case BalanceStrategyRange:
		result = sarama.BalanceStrategyRange
	default:
		result =defaultBalanceStrategy
	}
	return result

}

func ParseKafkaVersion(version string) sarama.KafkaVersion{
	ver,err:=sarama.ParseKafkaVersion(version)
	if err!=nil{
		ver = defaultKafkaVersion
	}
	return ver
}