package tcp

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
)

type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

// 监听请求
func ListenAndServe(address string) {
	listen, err := net.Listen("tcp", address)
	if err != nil {
		// log.Fatal 是 Go 语言标准库 log 包中的一个函数，用于记录日志并终止程序。
		// 它的作用是输出一条日志消息，然后调用 os.Exit(1) 强制退出程序。
		log.Fatal(fmt.Println("Error listening on", address))
	}
	defer listen.Close()
	log.Println("Listening on " + address)

	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal(fmt.Println("Error accepting: ", err))
		}
		go handleConnection(conn)
	}
}

// 处理请求
func handleConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		readString, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("Client disconnected")
			} else {
				log.Println("Error reading from", conn.RemoteAddr())
			}
		}
		b := []byte(readString)
		conn.Write(b)
	}
}

func main() {
	ListenAndServe(":8080")
}
