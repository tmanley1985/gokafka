package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
)

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
}

func main() {
	server := NewServer()

	server.Listen()
}