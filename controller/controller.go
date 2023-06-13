package controller

import (
	"encoding/json"
	"errors"
	"go-kafka/kafka"
	"go-kafka/model"
	"log"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

func NewOrder(c *gin.Context) {
	var order model.Order

	if err := c.ShouldBindJSON(&order); err != nil {
		c.JSON(http.StatusBadGateway, gin.H{
			"error": err.Error()})
		return
	} else {
		key, value, err := BinaryNewOrder(order)
		if err != nil {
			c.JSON(http.StatusBadGateway, gin.H{
				"error": err.Error()})
			return
		} else {
			go kafka.ProducerMessage("commerce_new_order", key, value)
			go kafka.ProducerMessage("commerce_send_email", key, value)
			c.JSON(http.StatusCreated, order)
		}
	}
}

func BinaryNewOrder(neworder model.Order) (key []byte, value []byte, erros error) {

	value, err := json.Marshal(neworder)
	if err != nil {
		log.Fatalf("Erro ao serializar objeto: %v", err)
		errors := errors.New("Erro ao serializar obj")
		return nil, nil, errors

	} else {
		key := []byte(strconv.FormatUint(neworder.Id, 10))
		return key, value, nil
	}

}
