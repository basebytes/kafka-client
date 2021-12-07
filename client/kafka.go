package client

import (
	"context"
	"github.com/Shopify/sarama"
)

func Produce(cfg *Config,tranceConfig TranceProducerConfigFunc,hook EventHook) (chan<- string,error){
	var (
		p  sarama.AsyncProducer
		results  chan string
		topic,addr,config,err=tranceConfig(cfg)
	)
	if err ==nil{
		p, err = sarama.NewAsyncProducer(addr, config)
	}
	if err!=nil{
		if hook!=nil{
			hook(EventTypeInitialError,cfg,err)
		}
		return nil,err
	}
	results =make(chan string,config.ChannelBufferSize)
	if (config.Producer.Return.Successes||config.Producer.Return.Errors)&&hook!=nil{
		go func() {
			for{
				select {
				case msg:=<-p.Successes():
					hook(EventTypeProduceMsgSuccess,msg,nil)
				case msg:=<-p.Errors():
					hook(EventTypeProduceMsgError,msg,nil)
				}
			}
		}()
	}
	go func() {
		defer p.Close()
		for r := range results {
			select {
				case p.Input() <- &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(r)}:
			}
		}
	}()
	return results,err
}


func Consume(cfg *Config,tranceConfig TranceConsumerConfigFunc,doCommit bool,hook EventHook) (<-chan string,error){
	var (
		results  chan string
		group sarama.ConsumerGroup
		groupId,topic,addr,config,err=tranceConfig(cfg)
	)
	if err==nil {
		group, err = sarama.NewConsumerGroup(addr, groupId, config)
	}
	if err != nil {
		if hook!=nil{
			hook(EventTypeInitialError,cfg,err)
		}
		return nil,err
	}
	results =make(chan string,config.ChannelBufferSize)
	if config.Consumer.Return.Errors&&hook!=nil{
		go func() {
			for err := range group.Errors() {
				hook(EventTypeConsumerGroupError,nil,err)
			}
		}()
	}
	go func() {
		defer group.Close()
		ctx := context.Background()
		handler := simpleConsumerGroupHandler{
			results: results,
			doCommit:doCommit,
			autoCommit:config.Consumer.Offsets.AutoCommit.Enable,
			hook:hook,
		}
		for{
			if err := group.Consume(ctx, []string{topic}, handler); err != nil &&hook!=nil {
				hook(EventTypeConsumerGroupError,nil,err)
			}
		}
	}()
	return results,err
}

type simpleConsumerGroupHandler struct{
	results chan string
	doCommit bool
	autoCommit bool
	hook EventHook
}

func (simpleConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (simpleConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h simpleConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		select {
			case h.results <- string(msg.Value):
				sess.MarkMessage(msg,"")
				if h.hook!=nil{
					h.hook(EventTypeConsumeMsgSuccess,msg,nil)
				}
				if !h.autoCommit &&h.doCommit{
					sess.Commit()
				}
		}
	}
	return nil
}