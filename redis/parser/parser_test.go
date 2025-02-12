package parser

import (
	"Godis-Self/redis/protocol"
	"fmt"
	"testing"
)

func TestParse1(t *testing.T) {
	replies, err := ParseBytes([]byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"))
	if err != nil {
		t.Fatalf("Error parsing bytes: %v", err)
	}
	if arrayReply, ok := replies[0].(*protocol.MultiBulkReply); ok {
		for _, item := range arrayReply.Args {
			fmt.Println(string(item))
		}
	}
}

func TestParse2(t *testing.T) {
	replies, err := ParseBytes([]byte("+OK\r\n"))
	if err != nil {
		t.Fatalf("Error parsing bytes: %v", err)
	}
	if arrayReply, ok := replies[0].(*protocol.StatusReply); ok {
		fmt.Println(string(arrayReply.Status))
	}
}

func TestParse3(t *testing.T) {
	replies, err := ParseBytes([]byte(":1\r\n"))
	if err != nil {
		t.Fatalf("Error parsing bytes: %v", err)
	}
	if arrayReply, ok := replies[0].(*protocol.IntReply); ok {
		fmt.Println(arrayReply.Code)
	}
}

func TestParse5(t *testing.T) {
	replies, err := ParseBytes([]byte("$4\r\nA\r\nB\r\n"))
	if err != nil {
		t.Fatalf("Error parsing bytes: %v", err)
	}
	if arrayReply, ok := replies[0].(*protocol.BulkReply); ok {
		fmt.Println(string(arrayReply.Arg))
	}
}
