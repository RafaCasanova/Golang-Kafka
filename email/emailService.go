package main

import (
	"context"
	"encoding/json"
	"fmt"
	"go-kafka/model"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func main() {

	brokerList := "localhost:19092,localhost:19093,localhost:19094" // Endereço dos brokers Kafka, separados por vírgula
	groupID := "email-service"                                      // ID do grupo de consumidores

	// Configuração do consumidor Kafka
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Group.Session.Timeout = 10 * time.Second
	config.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumer, err := sarama.NewConsumerGroup(strings.Split(brokerList, ","), groupID, config)
	if err != nil {
		log.Fatalf("Erro ao criar o consumidor Kafka: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Erro ao fechar o consumidor Kafka: %v", err)
		}
	}()

	client, err := sarama.NewClient(strings.Split(brokerList, ","), config)
	if err != nil {
		log.Fatalf("Erro ao criar o cliente Kafka: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("Erro ao fechar o cliente Kafka: %v", err)
		}
	}()

	// WaitGroup para aguardar o término do consumidor
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Rotina para consumir mensagens do Kafka
	go func() {
		defer wg.Done()

		for {
			err := consumer.Consume(context.Background(), []string{"commerce_send_email"}, &consumerGroupHandler{})
			if err != nil {
				log.Printf("Erro ao consumir mensagens do Kafka: %v", err)
			}
		}
	}()

	wg.Wait()

}

type consumerGroupHandler struct{}

func (h *consumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		var order model.Order

		err := json.Unmarshal(message.Value, &order)
		if err != nil {
			log.Printf("Erro ao desserializar mensagem: %v", err)
		} else {
			fmt.Printf("Mensagem recebida: Partition:%d Offset:%d Person:%+v\n",
				message.Partition, message.Offset, order)
		}

		session.MarkMessage(message, "")
	}

	return nil
}
