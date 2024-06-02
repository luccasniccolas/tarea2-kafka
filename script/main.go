package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

type Product struct {
	ProductName string `json:"productName"`
	Price       int    `json:"price"`
	Email       string `json:"email"`
}

func main() {
	// Leer el contenido del archivo dataset.json
	content, err := os.ReadFile("dataset.json")
	if err != nil {
		fmt.Println("Error al leer el archivo:", err)
		return
	}

	// Parsear los objetos JSON del archivo
	var products []Product
	err = json.Unmarshal(content, &products)
	if err != nil {
		fmt.Println("Error al parsear el JSON:", err)
		return
	}

	// Iterar sobre los productos y enviarlos por POST
	for _, product := range products {
		// Convertir el producto a JSON
		jsonData, err := json.Marshal(product)
		if err != nil {
			fmt.Println("Error al convertir el producto a JSON:", err)
			continue
		}

		// Realizar la solicitud POST
		resp, err := http.Post("http://localhost:8080/", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Println("Error al realizar la solicitud POST:", err)
			continue
		}
		defer resp.Body.Close()

		// Leer la respuesta del servidor
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			fmt.Println("Error al leer la respuesta del servidor:", err)
			continue
		}

		fmt.Println("Respuesta del servidor:", string(respBody))
	}
}
