package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go-kafka/routes"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	routes.HandleRequest()
}

func Consumer() {
	brokerList := "localhost:19092,localhost:19093,localhost:19094" // Endereço dos brokers Kafka, separados por vírgula
	topic := "meu-topico"                                           // Tópico Kafka a ser usado
	groupID := "asd-teste"                                          // ID do grupo de consumidores

	// Configuração do consumidor Kafka
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	//config.Version = sarama.V2_6_0_0
	go produceeras()
	// Criação do consumidor Kafka
	consumer, err := sarama.NewConsumerGroup(strings.Split(brokerList, ","), groupID, config)
	if err != nil {
		log.Fatalf("Erro ao criar o consumidor Kafka: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Erro ao fechar o consumidor Kafka: %v", err)
		}
	}()

	// WaitGroup para aguardar o término do consumidor
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Rotina para consumir mensagens do Kafka
	go func() {
		defer wg.Done()

		for {
			err := consumer.Consume(context.Background(), []string{topic}, &consumerGroupHandler{})
			if err != nil {
				log.Printf("Erro ao consumir mensagens do Kafka: %v", err)
			}

		}
	}()

	wg.Wait()
}

type consumerGroupHandler struct{}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	fmt.Println("Configuração do consumidor concluída")
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	fmt.Println("Limpeza do consumidor concluída")
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		person := Person{}

		err := json.Unmarshal(message.Value, &person)
		if err != nil {
			log.Printf("Erro ao desserializar mensagem: %v", err)
		} else {
			fmt.Printf("Mensagem recebida: Partition:%d Offset:%d Person:%+v\n",
				message.Partition, message.Offset, person)
		}

		session.MarkMessage(message, "")
	}

	return nil

	// for message := range claim.Messages() {
	// 	fmt.Printf("Mensagem recebida: Partition:%d Offset:%d Key:%s Value:%s\n",
	// 		message.Partition, message.Offset, string(message.Key), string(message.Value))
	// 	session.MarkMessage(message, "")
	// }

	// return nil
}

type Person struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func produceeras() {

	person := Person{
		Name: "Alice",
		Age:  30,
	}

	value, err := json.Marshal(person)
	if err != nil {
		log.Fatalf("Erro ao serializar objeto: %v", err)
	}

	brokerList := "localhost:19092" // Endereço dos brokers Kafka, separados por vírgula
	topic := "meu-topico"           // Tópico Kafka a ser usado

	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true

	// Criação do produtor Kafka
	producer, err := sarama.NewSyncProducer(strings.Split(brokerList, ","), producerConfig)
	if err != nil {
		log.Fatalf("Erro ao criar o produtor Kafka: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Erro ao fechar o produtor Kafka: %v", err)
		}
	}()

	// Envio de mensagens para o Kafka
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder("rune(1)"),
		Value: sarama.ByteEncoder(value),
	}

	// Envia a mensagem para o Kafka
	for i := 0; i < 5; i++ {
		time.Sleep(10 * time.Second)
		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			log.Printf("Erro ao enviar mensagem para o Kafka: %v", err)
		} else {
			fmt.Printf("Mensagem enviada para o Kafka. Partição: %d, Offset: %d\n", partition, offset)
		}
	}

}
