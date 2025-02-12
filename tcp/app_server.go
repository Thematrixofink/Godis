package tcp

import (
	"Godis-Self/lib/sync/atomic"
	"Godis-Self/lib/sync/wait"
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Client struct {
	Conn net.Conn
	// 当服务端开始发送数据时进入waiting, 阻止其它goroutine关闭连接
	// wait.Wait是作者编写的带有最大等待时间的封装:
	Waiting wait.Wait
}

// 应用层服务器的负责Echo的Handler
type EchoHandler struct {
	// 保存所有工作状态client的集合(把map当set用)
	// 需使用并发安全的容器
	activeConn sync.Map
	// 关闭状态标识位
	closing atomic.Boolean
}

// 处理连接，进行Echo
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {

	if h.closing.Get() {
		conn.Close()
		return
	}

	client := &Client{
		Conn: conn,
	}

	h.activeConn.Store(client, struct{}{})
	reader := bufio.NewReader(conn)
	for {
		readString, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				log.Println("client close")
				h.activeConn.Delete(client)
			} else {
				log.Println(err)
			}
			return
		}
		//发送数据之前，设置标志位，防止连接被关闭
		client.Waiting.Add(1)
		b := []byte(readString)
		conn.Write(b)
		client.Waiting.Done()
	}
}

// 关闭客户端连接
func (c *Client) Close() error {
	// 等待数据发送完毕然后关闭，超时时间设置为10s
	c.Waiting.WaitWithTimeout(10 * time.Second)
	c.Conn.Close()
	return nil
}

// 关闭服务器
func (h *EchoHandler) Close() error {
	log.Println("handler shutting down...")
	h.closing.Set(true)
	// 逐个关闭连接
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*Client)
		client.Close()
		return true
	})
	return nil
}
