package main

import (
	"log"
	"net/smtp"
	"strings"
)

func main() {
	auth := smtp.PlainAuth("", "tareasistemalaraya2002@gmail.com", "ztgm ooqh ieiq dyuh", "smtp.gmail.com")

	to := []string{"francisca.lopez@mail.udp.cl"}

	// Construir el mensaje con encabezados adecuados
	msg := strings.Join([]string{
		"From: tareasistemalaraya2002@gmail.com",
		"To: francisca.lopez@mail.udp.cl",
		"Bcc: francisca.lopez@mail.udp.cl",
		"Subject: Mensaje de Prueba",
		"",
		"Tu puedes mi princesita hermosa, por que eres la mejor del mundooooooo, te lo envie desde el codigo avisame si te llega, te adorooooooo",
	}, "\r\n")

	err := smtp.SendMail("smtp.gmail.com:587", auth, "tareasistemalaraya2002@gmail.com", to, []byte(msg))
	if err != nil {
		log.Fatalf("Error al enviar el correo: %v\n", err)
	}
}
