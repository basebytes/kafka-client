package examples

import (
	"fmt"
	"github.com/basebytes/kafka-client/client"
	"log"
	"sync"
	"testing"
	"time"
)

func TestProduce(t *testing.T){
	config:=&client.Config{
		Name:   "produce-test",
		Params: map[string]string{"producerTopic":"test","servers":"localhost:9092"},
	}
	produceChan,err:=client.Produce(config,TranceProducerConfig,ProducerLogHook)
	if err!=nil{
		panic(err)
	}
	wg:=sync.WaitGroup{}
	count:=10
	wg.Add(count)
	for i:=0;i<count;i++{
		produceChan<- fmt.Sprintf(`{"name":"test kafka msg-%d""}`,i)
		wg.Done()
	}
	wg.Wait()
	time.Sleep(time.Second)
}

func TestConsume(t *testing.T) {
	config:=&client.Config{
		Name:   "consume-test",
		Params: map[string]string{
			"consumerTopic":"test",
			"servers":"localhost:9092",
			"groupId":"kafka-client-test",

		},
	}

	messages,err:=client.Consume(config,TranceConsumerConfig,true,ConsumerLogHook)
	if err!=nil{
		panic(err)
	}
	timeout:=10*time.Second
	for {
		select {
			case message:=<-messages:
				log.Println(message)
			case <-time.NewTimer(timeout).C:
				log.Printf("no message received in %s",timeout)
				return
		}
	}
}

