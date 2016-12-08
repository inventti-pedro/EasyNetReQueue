package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type basicProperties struct {
	CorrelationId string
}

type easyNetQErrorMessage struct {
	BasicProperties basicProperties
	Message         string
}

type baseMessage struct {
	IdCorrelacao string
}

var (
	connectionString = flag.String("cs", "amqp://guest:guest@localhost:5672", "Necessária para conexão com o RabbitMQ")
	filaDestino      = flag.String("queue", "SendTeste", "Para onde a mensagem será enviada...")
	idCorrelacao     = flag.String("cid", "000-000-000", "ID de correlação da mensagem (Guid)")
	apagarMensagem   = flag.Bool("rm", false, "Apaga mensagem da fila de erro do EasyNetQ (caso encontrada e enviada com sucesso)")
)

func main() {

	flag.Parse()

	fmt.Println("cs:", *connectionString)
	fmt.Println("queue:", *filaDestino)
	fmt.Println("cid:", *idCorrelacao)

	log.SetOutput(os.Stderr)

	conn, err := amqp.Dial(*connectionString)
	if err != nil {
		log.Fatalf("connection.open: %s", err)
	}

	defer conn.Close()

	c, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel.open: %s", err)
	}

	msgs, err := c.Consume("EasyNetQ_Default_Error_Queue", "requeue", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("basic.consume: %v", err)
	}

	canal := make(chan bool, 1)

	go verificarMensagensErro(canal, c, msgs)

	<-canal
}

func verificarMensagensErro(done chan bool, channel *amqp.Channel, msgs <-chan amqp.Delivery) {
	set := make(map[string]bool)

	for d := range msgs {

		msg, err := matchesCorrelationId(d.Body)

		_, found := set[msg.BasicProperties.CorrelationId]

		if found {
			done <- true
			return
		}

		set[msg.BasicProperties.CorrelationId] = true

		if err == nil {
			fmt.Printf("\n\nEncontrado e enviando IdCorrelacao: %s para Fila: %s\n\n", *idCorrelacao, *filaDestino)

			erroEnvio := enviar(channel, *filaDestino, msg.Message)

			if erroEnvio == nil && *apagarMensagem {
				d.Ack(false)
			} else {
				d.Nack(false, true)
			}

			done <- true
			return
		}

		d.Nack(false, true)
	}
}

func matchesCorrelationId(jsonStream []byte) (easyNetQErrorMessage, error) {

	var m easyNetQErrorMessage
	var mcid baseMessage

	json.Unmarshal(jsonStream, &m)
	json.Unmarshal([]byte(m.Message), &mcid)

	if mcid.IdCorrelacao == *idCorrelacao {
		return m, nil
	}

	return m, errors.New("ID correlação não confere")
}

func enviar(c *amqp.Channel, queue string, mensagem string) error {

	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "",
		Body:         []byte(mensagem),
	}

	err := c.Publish("", queue, false, false, msg)
	if err != nil {
		log.Fatalf("basic.publish: %v", err)
	}

	return err
}
