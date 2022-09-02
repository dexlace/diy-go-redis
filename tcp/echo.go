package tcp

import (
	"bufio"
	"context"
	"go-redis/lib/logger"
	"go-redis/lib/sync/atomic"
	"go-redis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

/**
客户端关闭  实现系统的关闭接口
*/

func (client *EchoClient) Close() error {
	client.Waiting.WaitWithTimeout(10 * time.Second)
	_ = client.Conn.Close()
	return nil
}

type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

func MakeHandler() *EchoHandler {
	return &EchoHandler{}

}

/**
  处理新连接
*/

func (handler *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if handler.closing.Get() {
		_ = conn.Close()
	}

	// 包装成一个client
	client := EchoClient{
		Conn: conn,
	}

	// 存储新的客户端连接
	handler.activeConn.Store(client, struct{}{})
	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("Connection closed")
				handler.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}

		// 收什么 回写什么
		client.Waiting.Add(1)
		bytes := []byte(msg)
		_, _ = conn.Write(bytes)
		client.Waiting.Done()

	}

}

func (handler *EchoHandler) Close() error {
	logger.Info("handler shutting down")
	handler.closing.Set(true)
	handler.activeConn.Range(func(key, value interface{}) bool {
		client := key.(*EchoClient)
		_ = client.Conn.Close()
		return true
	})

	return nil

}
