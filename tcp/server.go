package tcp

import (
	"context"
	"go-redis/interface/tcp"
	"go-redis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Config struct {
	Address string
}

func ListenAndServeWithSignal(config *Config, handler tcp.Handler) error {

	closeChain := make(chan struct{})
	signalChain := make(chan os.Signal)
	// 系统发送的信号 关闭
	signal.Notify(signalChain, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// 如果发送了  则这里发送空结构体信号
	go func() {
		sig := <-signalChain
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChain <- struct{}{}
		}
	}()
	listen, err := net.Listen("tcp", config.Address)
	if err != nil {
		return err
	}
	logger.Info("start listen")
	ListenAndServe(listen, handler, closeChain)
	return nil
}

func ListenAndServe(listener net.Listener,
	handler tcp.Handler,
	closeChan <-chan struct{}) {
	go func() {
		<-closeChan
		logger.Info("shutting down")
		_ = listener.Close()
		_ = handler.Close()
	}()

	// 退出时必须关闭
	defer func() {
		_ = listener.Close()
		_ = handler.Close()
	}()

	ctx := context.Background()
	var waitDone sync.WaitGroup
	for true {
		conn, err := listener.Accept()
		if err != nil {
			break
		}

		logger.Info("accepted link")
		waitDone.Add(1)
		go func() {
			// 一定得执行
			defer func() {
				waitDone.Done()
			}()
			handler.Handle(ctx, conn)
		}()
	}

	// 一旦break
	// 等待已经生成的所有协程退出
	waitDone.Wait()

}
