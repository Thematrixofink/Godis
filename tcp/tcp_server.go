package tcp

import (
	"Godis-Self/interface/tcp"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config TCP服务器设置
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// 监听并提供服务，并在收到 closeChan 发来的关闭通知后关闭
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {

	// <-chan struct{} 作用是声明
	// 声明closeChan 是一种特殊的通道：只接收（Receive-Only）通道，接受的是空结构体（不占用内存，一般用来信息传递）

	// 确保  正常关闭  状态下可以关闭所有的资源
	go func() {
		// 从通道读取
		<-closeChan
		log.Println("close tcp server listener")
		// 停止监听
		_ = listener.Close()
		// 关闭应用层服务器
		_ = handler.Close()
	}()

	// 确保  异常状态  下也可以关闭所有的资源
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	//	Q：为什么不只留一个defer来关闭？
	//  A：程序无法在收到信号时主动关闭，那么只能在出现异常或者关闭程序时关闭资源

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println(fmt.Printf("tcp server listener accept err: %v\n", err))
			break
		}
		log.Println("accept link")
		// 活跃的连接数+1
		waitDone.Add(1)
		// 开启一个线程去处理这个请求
		go func() {
			// 处理完标志处理完毕
			defer waitDone.Done()
			handler.Handle(ctx, conn)
		}()
	}
	// 等待所有 Goroutine 完成
	waitDone.Wait()
}

// ListenAndServeWithSignal 监听中断信号并通过 closeChan 通知服务器关闭
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{})
	// 创建一个管道，数据类型为系统信号
	sigOS := make(chan os.Signal)
	// 指定sigOS需要监听的系统信号类型
	signal.Notify(sigOS, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigOS
		// 这里又采用了一个switch case来进一步确保只有特定信号关闭
		// 便于之后扩展
		switch sig {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP:
			closeChan <- struct{}{}
		}
	}()
	listen, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	log.Printf("start tcp server listener at %v\n", cfg.Address)
	ListenAndServe(listen, handler, closeChan)
	return nil
}
