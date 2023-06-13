package kafka

import (
	"fmt"
	"log"
	"strings"

	"github.com/Shopify/sarama"
)

var BROKERLIST = "localhost:19092,localhost:19093,localhost:19094"
var TOPIC = "commerce_new_order,commerce_send_email"

func StartKafka() {
	CreateTopics(TOPIC)
}

func ProducerMessage(topic string, key, value []byte) {
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.Idempotent = true

	// Criação do produtor Kafka
	producer, err := sarama.NewSyncProducer(strings.Split(BROKERLIST, ","), producerConfig)
	if err != nil {
		log.Fatalf("Erro ao criar o produtor Kafka: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Erro ao fechar o produtor Kafka: %v", err)
		}
	}()
	fmt.Println("enviando mensagem")
	// Envio de mensagens para o Kafka
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	// Envia a mensagem para o Kafka
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Printf("Erro ao enviar mensagem para o Kafka: %v", err)
	} else {
		fmt.Printf("Mensagem enviada para o Kafka. Partição: %d, Offset: %d\n", partition, offset)
	}
}

func CreateTopics(topic string) {
	// Configuração do administrador do Kafka
	admin, err := sarama.NewClusterAdmin(strings.Split(BROKERLIST, ","), nil)
	if err != nil {
		log.Fatalf("Erro ao criar o administrador do Kafka: %v", err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Printf("Erro ao fechar o administrador do Kafka: %v", err)
		}
	}()

	// Configurações do tópico
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     5,
		ReplicationFactor: 2,
	}

	// Criação do tópico
	err = admin.CreateTopic(topic, topicDetail, false)
	if err != nil {
		log.Fatalf("Erro ao criar o tópico: %v", err)
	}

	log.Println("Tópico criado com sucesso!")
}
