package protobufmessaging

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/mkyas/miniraft"
	"google.golang.org/protobuf/proto"
)

const (
	MessageLengthSize = 8
	MaxMessageSize    = 1024 * 1024 // 1MB should be plenty
	ReadTimeout       = 10 * time.Second
	WriteTimeout      = 10 * time.Second
)

func logf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func handleError(err error) error {
	if err != nil && err != io.EOF {
		logf("Error: %s\n", err)
	}
	return err
}

func ReceiveMessage(conn net.Conn) (*miniraft.Raft, error) {
	conn.SetReadDeadline(time.Now().Add(ReadTimeout))

	reader := bufio.NewReader(conn)

	bs := make([]byte, MessageLengthSize)
	_, err := io.ReadFull(reader, bs)
	if err = handleError(err); err != nil {
		return nil, err
	}

	numBytes := int(binary.BigEndian.Uint64(bs))
	if numBytes > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d", numBytes)
	}

	data := make([]byte, numBytes)
	_, err = io.ReadFull(reader, data)
	if err = handleError(err); err != nil {
		return nil, err
	}

	message := &miniraft.Raft{}
	err = proto.Unmarshal(data, message)
	if err = handleError(err); err != nil {
		return nil, err
	}

	logf("Received %s from %s\n", message, conn.RemoteAddr().String())
	return message, nil
}

func SendMessage(conn net.Conn, message *miniraft.Raft) error {
	conn.SetWriteDeadline(time.Now().Add(WriteTimeout))

	data, err := proto.Marshal(message)
	if err = handleError(err); err != nil {
		return err
	}

	logf("Sending %s to %s\n", message, conn.RemoteAddr().String())

	bs := make([]byte, MessageLengthSize)
	binary.BigEndian.PutUint64(bs, uint64(len(data)))
	_, err = conn.Write(bs)
	if err = handleError(err); err != nil {
		return err
	}

	_, err = conn.Write(data)
	return handleError(err)
}
