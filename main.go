package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Header struct {
	Size 		int32
	APIKey 		int16
	APIVersion	int16
}

type APIVersion struct {
	CorrelationId			int32
	ClientID				string
	ClientSoftwareName		string
	ClientSoftwareVersion	string
}

func readAPIVersion(r io.Reader) APIVersion {

	var version APIVersion
	binary.Read(r, binary.BigEndian, &version.CorrelationId)
	
	// I think you're actually supposed to get this as a varint.
	var size int16

	binary.Read(r, binary.BigEndian, &size)
	clientId := make([]byte, size)
	binary.Read(r, binary.BigEndian, &clientId)

	binary.Read(r, binary.BigEndian, &size)
	clientSoftwareName := make([]byte, size)
	binary.Read(r, binary.BigEndian, &clientSoftwareName)

	clientSoftwareVersion, _ := io.ReadAll(r)

	fmt.Println("Software name: ", string(clientSoftwareName))
	fmt.Println("Software version: ", string(clientSoftwareVersion))

	fmt.Println("####################$##############")
	return APIVersion{
		ClientID: string(clientId),
		ClientSoftwareName: string(clientSoftwareName),
		ClientSoftwareVersion: string(clientSoftwareVersion),
	}
}



type Message struct {
	data []byte
}

type Server struct {
	// These will hold the consumer group
	consumerGroupOffsets map[string]int

	buffer []Message
	ln net.Listener 
}

func NewServer() *Server {
	return &Server{
		consumerGroupOffsets: make(map[string]int),
		buffer: make([]Message, 0),
	}
}

func (s *Server) Start() error {
	return nil
}

func (s *Server) Listen() error {
	ln, err := net.Listen("tcp", ":9092")

	if err != nil {
		return err
	}

	s.ln = ln

	for {
		conn, err := ln.Accept()

		if err != nil {

			if err == io.EOF {
				return nil
			}

			slog.Error("Server accept error", "err", err)
			
		}

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	fmt.Println("Started new connection", conn.RemoteAddr())

	buf := make([]byte, 1024)

	for {

		numOfBytesRead, err := conn.Read(buf)

		if err != nil {
			if err == io.EOF {
				return
			}

			slog.Error("connection read error", "err", err)
			return
		}

		fmt.Println(string(buf[:numOfBytesRead]))

		msg := buf[:numOfBytesRead]

		fmt.Println(msg)
		r := bytes.NewReader(msg)

		var header Header

		binary.Read(r, binary.BigEndian, &header)

		fmt.Println("Header is: ")
		fmt.Println(header.APIKey)
		fmt.Println(header.APIVersion)
		fmt.Println("&&&&&&&&&&&&&&&&&&&&&#&&&&&&")

		request := readAPIVersion(r)

		fmt.Println(request)
	}
}

func main() {
	server := NewServer()
	go func ()  {
		log.Fatal(server.Listen())
	}()

	time.Sleep(time.Second)

	// fmt.Println("consuming...")
	// consume()

	fmt.Println("producing...")
	produce()
}


func produce() error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
	if err != nil {
		return err
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "someTopic"
	for _, word := range []string{"Welcome", "to", "the", "Confluent", "Kafka", "Golang", "client"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)

	return nil
}

func consume()  error {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "myGroup",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return err
	}

	c.SubscribeTopics([]string{"someTopic"}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		msg, err := c.ReadMessage(time.Second)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else if !err.(kafka.Error).IsTimeout() {
			// The client will automatically try to recover from all errors.
			// Timeout is not considered an error because it is raised by
			// ReadMessage in absence of messages.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	return c.Close()
}