package main

import (
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type Data struct {
	ProductName string `json:"productName"`
	Price       int    `json:"price"`
	Email       string `json:"email"`
}

type Order struct {
	ID          int64  `json:"id"`
	ProductName string `json:"productName"`
	Price       int    `json:"price"`
	Email       string `json:"email"`
}

var idCounter int64

func generateUniqueID() int64 {
	return atomic.AddInt64(&idCounter, 1)
}

func orderHandler(kafkaWriter *kafka.Writer) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) { // nos aseguramos que la peticion sea POST
		if r.Method != http.MethodPost {
			http.Error(w, "ONLY POST", http.StatusMethodNotAllowed)
			return
		}

		var data Data
		err := json.NewDecoder(r.Body).Decode(&data)
		if err != nil {
			http.Error(w, "Solicitud incorrecta", http.StatusBadRequest)
			return
		}

		order := Order{
			ID:          generateUniqueID(),
			ProductName: data.ProductName,
			Price:       data.Price,
			Email:       data.Email,
		}

		// codificamos a json la orden para enviarla
		orderMSG, err := json.Marshal(order)
		if err != nil {
			http.Error(w, "Error al crear la orden", http.StatusInternalServerError)
			return
		}

		// Crear el mensaje Kafka
		msg := kafka.Message{
			Key:   []byte(strconv.FormatInt(order.ID, 10)),
			Value: orderMSG,
		}

		// Enviar el mensaje a Kafka
		err = kafkaWriter.WriteMessages(r.Context(), msg)
		if err != nil {
			fmt.Println(err)
			http.Error(w, "Error al enviar la orden a Kafka", http.StatusInternalServerError)
			return
		}
		currentTime := time.Now()
		formattedTime := currentTime.Format("2006-01-02 15:04:05")
		fmt.Println("Mensaje enviado:", formattedTime)

		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("Su compra fue recibida y enviada al sistema"))
	}
}

// funcion para generar un productor
func getKafkaWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond, // Reducir BatchTimeout a 10ms
	}
}

func main() {

	topic := "orders"
	kafkaWriter := getKafkaWriter(topic)

	defer kafkaWriter.Close()

	http.HandleFunc("/", orderHandler(kafkaWriter))

	// Run the web server.
	fmt.Println("Iniciando servidor")
	log.Fatal(http.ListenAndServe(":8080", nil))

}
