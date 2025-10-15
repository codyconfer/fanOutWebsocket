package main

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/reactivex/rxgo/v2"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func writePump(conn *websocket.Conn, observable rxgo.Observable) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("error closing websocket connection: %v", err)
		}
	}()

	for item := range observable.Observe() {
		if item.Error() {
			log.Printf("error from observable: %v", item.E)
			return
		}
		message, ok := item.V.(Message)
		if !ok {
			log.Printf("invalid message type from observable: %T", item.V)
			continue
		}
		if err := conn.WriteMessage(message.Type, message.Payload); err != nil {
			log.Println(err)
			return
		}
	}
}

func readPump(conn *websocket.Conn, operator chan<- rxgo.Item) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("error closing websocket connection: %v", err)
		}
	}()
	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("error: %v", err)
			}
			break
		}
		operator <- rxgo.Of(Message{Type: messageType, Payload: message})
	}
}

func websocketHandler(operator chan<- rxgo.Item, observable rxgo.Observable) gin.HandlerFunc {
	return func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			log.Println(err)
			return
		}
		go writePump(conn, observable)
		go readPump(conn, operator)
	}
}

func NewRouter(operator chan<- rxgo.Item, observable rxgo.Observable) *gin.Engine {
	router := gin.Default()
	router.GET("/ws", websocketHandler(operator, observable))
	return router
}
