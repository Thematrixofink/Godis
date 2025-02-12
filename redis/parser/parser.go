package parser

import (
	"Godis-Self/interface/redis"
	"Godis-Self/redis/protocol"
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
)

type Payload struct {
	Data  redis.Reply
	Error error
}

// 流式处理
// ParseStream 通过 io.Reader 读取数据并将结果通过 channel 将结果返回给调用
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse(reader, ch)
	return ch
}

// 解析[]byte，然后一次性返回Redis的所有回复
// ParseBytes reads data from []byte and return all replies
func ParseBytes(data []byte) ([]redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse(reader, ch)
	var results []redis.Reply
	for payload := range ch {
		if payload == nil {
			return nil, errors.New("no protocol")
		}
		if payload.Error != nil {
			if payload.Error == io.EOF {
				break
			}
			return nil, payload.Error
		}
		results = append(results, payload.Data)
	}
	return results, nil
}

// 只返回第一个回复
// ParseOne reads data from []byte and return the first payload
func ParseOne(data []byte) (redis.Reply, error) {
	ch := make(chan *Payload)
	reader := bytes.NewReader(data)
	go parse(reader, ch)
	payload := <-ch // parse0 will close the channel
	if payload == nil {
		return nil, errors.New("no protocol")
	}
	return payload.Data, payload.Error
}

// 解析函数
func parse(rawReader io.Reader, ch chan<- *Payload) {

	reader := bufio.NewReader(rawReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			ch <- &Payload{Error: err}
			close(ch)
			return
		}

		// 行以 \r\n 结尾，因此一个有效的行至少需要包含 \r 和 \n 这两个字符。
		// 如果长度小于或等于 2，说明这行可能是空行或只包含换行符，因此不需要进一步处理
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			continue
		}
		// 删除最后的换行符
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'})
		switch line[0] {
		// 简单字符串，用于返回一些状态信息
		case '+':
			ch <- &Payload{
				Data: protocol.MakeStatusReply(string(line[1:])),
			}
		// 错误
		case '-':
			ch <- &Payload{
				Data: protocol.MakeErrReply(string(line[1:])),
			}
		// 整数
		case ':':
			i, err := strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				protocolError(ch, "illegal number "+string(line[1:]))
				continue
			}
			ch <- &Payload{
				Data: protocol.MakeIntReply(i),
			}
		// Bulk String
		case '$':
			err := parseBulkString(line, reader, ch)
			if err != nil {
				ch <- &Payload{Error: err}
				close(ch)
				return
			}
		// Array
		case '*':
			err := parseArray(line, reader, ch)
			if err != nil {
				ch <- &Payload{Error: err}
				close(ch)
				return
			}
		default:
		}
	}
}

// 解析BulkString
func parseBulkString(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	// line 是 $123 这种形式，最后的换行符也被删去了
	num, err := strconv.ParseInt(string(line[1:]), 10, 64)
	// 特殊情况
	if err != nil || num < -1 {
		protocolError(ch, "illegal bulk number "+string(line[1:]))
		return nil
	} else if num == -1 {
		ch <- &Payload{
			Data: protocol.MakeNullBulkReply(),
		}
		return nil
	}
	// 处理正常情况
	body := make([]byte, num+2)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return err
	}
	ch <- &Payload{
		Data: protocol.MakeBulkReply(body[:len(body)-2]),
	}
	return nil
}

// 解析Array
func parseArray(line []byte, reader *bufio.Reader, ch chan<- *Payload) error {
	//line 是 *123 这种形式，最后的换行符也被删去了
	num, err := strconv.ParseInt(string(line[1:]), 10, 64)
	// 特殊情况
	if err != nil || num < 0 {
		protocolError(ch, "illegal array number "+string(line[1:]))
		return nil
	} else if num == 0 {
		ch <- &Payload{
			Data: protocol.MakeEmptyMultiBulkReply(),
		}
		return nil
	}
	// 处理正常情况
	bulkStrings := make([][]byte, 0, num)
	for i := int64(0); i < num; i++ {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(line)
		if length < 4 || line[length-2] != '\r' || line[0] != '$' {
			protocolError(ch, "illegal bulk string header "+string(line))
			break
		}
		bodyLen, err := strconv.ParseInt(string(line[1:length-2]), 10, 64)
		if err != nil || bodyLen < -1 {
			protocolError(ch, "illegal bulk number "+string(line[1:length-2]))
			break
		} else if bodyLen == -1 {
			bulkStrings = append(bulkStrings, []byte{})
		} else {
			body := make([]byte, bodyLen+2)
			_, err = io.ReadFull(reader, body)
			if err != nil {
				return err
			}
			bulkStrings = append(bulkStrings, body[:len(body)-2])
		}
	}
	ch <- &Payload{
		Data: protocol.MakeMultiBulkReply(bulkStrings),
	}
	return nil
}

func protocolError(ch chan<- *Payload, msg string) {
	err := errors.New("protocol error: " + msg)
	ch <- &Payload{Error: err}
}
