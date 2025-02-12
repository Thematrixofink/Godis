package main

import "Godis-Self/tcp"

func main() {
	cfg := tcp.Config{
		"127.0.0.1:8080",
		1024,
		10,
	}
	handler := tcp.EchoHandler{}
	_ = tcp.ListenAndServeWithSignal(&cfg, &handler)
}
