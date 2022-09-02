package parser

import (
	"bufio"
	"errors"
	"go-redis/interface/resp"
	"go-redis/lib/logger"
	"go-redis/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data  resp.Reply
	Error error
}

type readState struct {
	// 是否读多行
	readingMultiLine bool
	// 期待的参数数量
	expectedArgsCount int
	// 消息类型
	msgType byte
	// 参数数组
	args [][]byte
	// 每一行的长度
	bulkLen int64
}

func (s *readState) finished() bool {
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch)
	return ch
}

func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		// 为了因程序异常退出
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		// 读一行
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state)
		if err != nil {
			// 遇到io错误，停止读
			if ioErr {
				ch <- &Payload{
					Error: err,
				}
				// 关闭管道  返回
				close(ch)
				return
			}
			// 协议错误 重置读的状态 继续读取下一行
			ch <- &Payload{
				Error: err,
			}
			state = readState{}
			continue
		}

		// 如果不是多行模式或者数组模式 即读多行为false
		if !state.readingMultiLine {
			// 说明多行解析模式未被打开
			// 比如 *3\r\n$3\r\nSET\r\nKEY$5\r\nVALUE\r\n 现在在解析*3\r\n
			if msg[0] == '*' {
				// 表示的是多行模式  现在要解析头
				err = parseMultiBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Error: errors.New("protocol error: " + string(msg)),
					}
					// 重置状态
					state = readState{}
					continue
				}
				// 如果期望解析的参数个数是0  则返回空
				if state.expectedArgsCount == 0 {
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					// 重置状态
					state = readState{}
					continue
				}
			} else if msg[0] == '$' {
				// 多行模式 后面跟实际要发送的字符串
				// 比如 $7\r\ndexlace\r\n
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Error: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.bulkLen == -1 { // null bulk reply
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else {
				// single line reply
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{
					Data:  result,
					Error: err,
				}
				state = readState{} // reset state
				continue
			}
		} else {
			// receive following bulk reply
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Error: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // reset state
				continue
			}
			// if sending finished
			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data:  result,
					Error: err,
				}
				state = readState{}
			}
		}
	}

}

// 这个不是解析 是读取
// 按照是否有$来区分读取一行的值  因为只有$才会指定后面的字节数
// 读一行  并返回读到的值、是否有io错误、错误
func readLine(reader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	// 没有读到$数字,此时state.bulkLen == 0
	if state.bulkLen == 0 {
		// 以'\n'分割 todo 分割之后还带\n?
		msg, err = reader.ReadBytes('\n')
		if err != nil {
			// io错误
			return nil, true, err
		}
		// 真实的redis解析协议是\r\n 所以得判断倒数第二个字符是否是\r
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
	} else {
		// 该行有$数字开头，则读取时的长度为bulkLen+2  加上了\r\n
		msg = make([]byte, state.bulkLen+2)
		_, err := io.ReadFull(reader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 ||
			msg[len(msg)-2] != '\r' ||
			msg[len(msg)-1] != '\n' {
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0
	}

	return msg, false, nil

}

// 读数组时，读到它的头 即第一行
// 解析数组头 *3\r\n
func parseMultiBulkHeader(msg []byte, state *readState) error {
	var err error
	var expectedLine uint64
	// 头一行 其实就是3
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 {
		// 如果读到的是0行  则将读状态指针的期望读取参数个数变为0
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// first line of multi bulk reply
		// 将*作为类型 即表示读到的是数组  传递的是指针表示下一个需要根据这个解析
		state.msgType = msg[0]
		// 读多行为true
		state.readingMultiLine = true
		// 期望的参数个数
		state.expectedArgsCount = int(expectedLine)
		// 参数
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// 读多行时的头
// $4\r\nPING\r\n
// 读的是$4\r\n
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	// 所以state.bulkLen是读多行时的长度
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		// 即$，表示多行
		state.msgType = msg[0]
		// 此时为多行
		state.readingMultiLine = true
		// 参数个数为1
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

// 这里读取body  上面的两个方法解析的是头
// $3\r\nSET\r\n$3\r\nKEY\r\n$5\r\n\VALUE\r\n
// PING\r\n
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		// bulk reply
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 { // null bulk in multi bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}

// '+'  '-'  ':'
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	// 去掉\r\n得到
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+': // status reply
		result = reply.MakeStatusReply(str[1:])
	case '-': // err reply
		result = reply.MakeErrReply(str[1:])
	case ':': // int reply
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil {
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	}
	return result, nil
}
