package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	statusMap sync.Map
)

type Order struct {
	ID          int64  `json:"id"`
	ProductName string `json:"productName"`
	Price       int    `json:"price"`
	Email       string `json:"email"`
	Status      string `json:"status"`
}

// Creación de un consumidor
func getKafkaReader(topic, groupID string) *kafka.Reader {
	brokers := strings.Split("localhost:9092,localhost:9093,localhost:9094", ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:          brokers,
		GroupID:          groupID,
		Topic:            topic,
		MinBytes:         10e3,                   // 10KB
		MaxBytes:         10e6,                   // 10MB
		MaxWait:          500 * time.Millisecond, // Reducir MaxWait a 500ms
		ReadBatchTimeout: 500 * time.Millisecond, // Reducir ReadBatchTimeout a 500ms
		ReadLagInterval:  1 * time.Second,        // Ajustar el intervalo de retraso de lectura a 1s
		CommitInterval:   1 * time.Second,        // Reducir CommitInterval a 1s
	})
}

func main() {
	consumerGroup := "process-notifications" // Creamos un consumer group que se encarga de leer las notificaciones
	topic := "notifications"

	rNotification := getKafkaReader(topic, consumerGroup)
	defer rNotification.Close()

	go func() {
		for {
			m, err := rNotification.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Error al leer el mensaje desde notifications: %v\n", err)
				continue
			}

			var order Order
			err = json.Unmarshal(m.Value, &order)
			if err != nil {
				log.Printf("Error al decodificar el JSON: %v\n", err)
				continue
			}

			statusMap.Store(order.ID, order.Status)

			currentTime := time.Now()
			formattedTime := currentTime.Format("2006-01-02 15:04:05")
			log.Printf("Mail enviado: Pedido %d con estado %s a las %s", order.ID, order.Status, formattedTime)
		}
	}()

	http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
		idParam := r.URL.Query().Get("id")
		if idParam == "" {
			http.Error(w, "Por favor ingresa un id", http.StatusBadRequest)
			return
		}

		id, err := strconv.ParseInt(idParam, 10, 64)
		if err != nil {
			http.Error(w, "Parámetro inválido", http.StatusBadRequest)
			return
		}

		if status, ok := statusMap.Load(id); ok {
			w.Write([]byte(status.(string)))
		} else {
			http.Error(w, "ID no encontrado", http.StatusNotFound)
		}
	})

	log.Println("Escuchando el puerto 8081")
	log.Fatal(http.ListenAndServe(":8081", nil))
}
