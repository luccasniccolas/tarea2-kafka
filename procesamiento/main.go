package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"strconv"
	"strings"
	"time"
)

type OrderStatus struct {
	ID          int64  `json:"id"`
	ProductName string `json:"productName"`
	Price       int    `json:"price"`
	Email       string `json:"email"`
	Status      string `json:"status"`
}

// Creación de un consumer
func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
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

// Creación de productor
func getKafkaWriter(kafkaURL, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(kafkaURL),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond, // Reducir BatchTimeout a 10ms
	}
}

func main() {
	brokers := "localhost:9092"
	consumerGroup := "process-order" // Creamos un consumer group que se encarga de leer las ordenes para cambiarlas de estado
	topics := []string{"orders", "status", "notifications"}

	rOrders := getKafkaReader(brokers, topics[0], consumerGroup)
	rOrdersStatus := getKafkaReader(brokers, topics[1], consumerGroup)
	wNotifications := getKafkaWriter(brokers, topics[2])
	wStatus := getKafkaWriter(brokers, topics[1])

	defer wNotifications.Close()
	defer rOrdersStatus.Close()
	defer rOrders.Close()

	go func() {
		for {
			m, err := rOrders.ReadMessage(context.Background())
			if err != nil {
				log.Fatalf("Error al leer el mensaje desde orders: %v\n", err)
			}

			var order OrderStatus
			err = json.Unmarshal(m.Value, &order) // Almacenamos los datos JSON en order
			if err != nil {
				log.Fatalf("Error al decodificar el JSON: %v\n", err)
			}

			order.Status = "recibido"
			sendToNotifications(wNotifications, order)
			currentTime := time.Now()
			formattedTime := currentTime.Format("2006-01-02 15:04:05")
			fmt.Println(formattedTime, order)
			time.Sleep(5 * time.Second)
			sendToStatus(wStatus, order)
		}
	}()

	go func() {
		for {
			m, err := rOrdersStatus.ReadMessage(context.Background())
			if err != nil {
				log.Fatalf("Error al leer el mensaje desde status: %v\n", err)
			}

			var order OrderStatus
			err = json.Unmarshal(m.Value, &order) // Almacenamos los datos JSON en order
			if err != nil {
				log.Fatalf("Error al decodificar el JSON: %v\n", err)
			}

			updateOrderStatus(&order)
			sendToNotifications(wNotifications, order)
			currentTime := time.Now()
			formattedTime := currentTime.Format("2006-01-02 15:04:05")
			fmt.Println(formattedTime, order)
			time.Sleep(5 * time.Second) // Mandamos nuevamente a status después de 5 segundos
			if order.Status != "finalizado" {
				sendToStatus(wStatus, order)
			} else {
				// Enviamos notificación final y no reenviamos a status
				sendToNotifications(wNotifications, order)
			}
		}
	}()

	select {}
}

func sendToNotifications(writer *kafka.Writer, order OrderStatus) {
	notification, err := json.Marshal(order) // Pasamos a JSON el evento a enviar
	if err != nil {
		log.Fatalf("Error al codificar el JSON: %v\n", err)
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(strconv.FormatInt(order.ID, 10)),
		Value: notification,
	})

	if err != nil {
		log.Fatalf("Error al escribir la notificación: %v\n", err)
	}
}

func updateOrderStatus(order *OrderStatus) {
	switch order.Status {
	case "recibido":
		order.Status = "preparando"
	case "preparando":
		order.Status = "entregando"
	case "entregando":
		order.Status = "finalizado"
	}
}

func sendToStatus(writer *kafka.Writer, order OrderStatus) {
	m, err := json.Marshal(order)
	if err != nil {
		log.Fatalf("Error al crear la orden: %v\n", err)
	}

	err = writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(strconv.FormatInt(order.ID, 10)),
		Value: m,
	})

	if err != nil {
		log.Fatalf("Error al enviar la actualización de estado: %v\n", err)
	}
}
