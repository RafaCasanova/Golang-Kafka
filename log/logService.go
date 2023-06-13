package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func main() {

	brokerList := "localhost:19092,localhost:19093,localhost:19094" // Endereço dos brokers Kafka, separados por vírgula
	groupID := "log-service"                                        // ID do grupo de consumidores

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

	// Obtém os metadados do cluster Kafka
	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("Erro ao obter a lista de tópicos: %v", err)
	}

	filteredTopics := make([]string, 0)
	for _, topic := range topics {
		if !isDefaultTopic(topic) {
			filteredTopics = append(filteredTopics, topic)
		}
	}

	// WaitGroup para aguardar o término do consumidor
	wg := &sync.WaitGroup{}
	wg.Add(1)

	// Rotina para consumir mensagens do Kafka
	go func() {
		defer wg.Done()

		for {
			err := consumer.Consume(context.Background(), []string(filteredTopics), &consumerGroupHandler{})
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
	fileLog, err := os.OpenFile("testlogfile.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer fileLog.Close()
	fmt.Println("consumer log kafka started")
	for message := range claim.Messages() {
		log.SetOutput(fileLog)

		log.Printf("Mensagem recebida: Topic:%s Partition:%d Offset:%d Key:%s Value:%s\n",
			message.Topic, message.Partition, message.Offset, string(message.Key), string(message.Value))
		session.MarkMessage(message, "")
	}

	return nil
}

func isDefaultTopic(topic string) bool {
	return topic == "__consumer_offsets" || topic == "__transaction_state"
}
