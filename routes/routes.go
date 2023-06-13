package routes

import (
	"go-kafka/controller"

	"github.com/gin-gonic/gin"
)

func HandleRequest() {
	r := gin.Default()

	r.POST("order", controller.NewOrder)
	r.Run(":9090")
}
