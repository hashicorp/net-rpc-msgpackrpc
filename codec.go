// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MIT

package msgpackrpc

import (
	"bufio"
	"io"
	"net/rpc"
	"sync"

	"github.com/hashicorp/go-msgpack/v2/codec"
)

var (
	// msgpackHandle is shared handle for decoding
	msgpackHandle = &codec.MsgpackHandle{}
)

// MsgpackCodec implements the rpc.ClientCodec and rpc.ServerCodec
// using the msgpack encoding
type MsgpackCodec struct {
	closed    bool
	conn      io.ReadWriteCloser
	bufR      *bufio.Reader
	bufW      *bufio.Writer
	enc       *codec.Encoder
	dec       *codec.Decoder
	writeLock sync.Mutex
}

// NewCodec returns a MsgpackCodec that can be used as either a Client or Server
// rpc Codec using a default handle. It also provides controls for enabling and
// disabling buffering for both reads and writes.
func NewCodec(bufReads, bufWrites bool, conn io.ReadWriteCloser) *MsgpackCodec {
	return NewCodecFromHandle(bufReads, bufWrites, conn, msgpackHandle)
}

// NewCodecFromHandle returns a MsgpackCodec that can be used as either a Client
// or Server rpc Codec using the passed handle. It also provides controls for
// enabling and disabling buffering for both reads and writes.
func NewCodecFromHandle(bufReads, bufWrites bool, conn io.ReadWriteCloser,
	h *codec.MsgpackHandle) *MsgpackCodec {
	cc := &MsgpackCodec{
		conn: conn,
	}
	if bufReads {
		cc.bufR = bufio.NewReader(conn)
		cc.dec = codec.NewDecoder(cc.bufR, h)
	} else {
		cc.dec = codec.NewDecoder(cc.conn, h)
	}
	if bufWrites {
		cc.bufW = bufio.NewWriter(conn)
		cc.enc = codec.NewEncoder(cc.bufW, h)
	} else {
		cc.enc = codec.NewEncoder(cc.conn, h)
	}
	return cc
}

func (cc *MsgpackCodec) ReadRequestHeader(r *rpc.Request) error {
	return cc.read(r)
}

func (cc *MsgpackCodec) ReadRequestBody(out interface{}) error {
	return cc.read(out)
}

// WriteResponse encodes the provided *rpc.Response header and its associated
// body, writing both to the underlying connection using Msgpack encoding.
//
// If an error occurs at any stage of encoding the header, encoding the body, or
// flushing the buffered writer (if one is used), the codec will close the
// underlying connection. This is done because the net/rpc package (which is
// frozen) does not propagate errors returned by WriteResponse, but optionally
// logs them. By closing the connection on error, we ensure that the codec (and
// underlying connection) is not used further in an inconsistent state, and
// subsequent calls immediately return io.EOF.
//
// Note: It is assumed that once an error is encountered, further communication
// using this codec is unsafe.
func (cc *MsgpackCodec) WriteResponse(r *rpc.Response, body interface{}) error {
	cc.writeLock.Lock()
	defer cc.writeLock.Unlock()
	if cc.closed {
		return io.EOF
	}
	if err := cc.enc.Encode(r); err != nil {
		cc.Close()
		return err
	}
	if err := cc.enc.Encode(body); err != nil {
		cc.Close()
		return err
	}
	if cc.bufW != nil {
		if err := cc.bufW.Flush(); err != nil {
			cc.Close()
			return err
		}
	}
	return nil
}

func (cc *MsgpackCodec) ReadResponseHeader(r *rpc.Response) error {
	return cc.read(r)
}

func (cc *MsgpackCodec) ReadResponseBody(out interface{}) error {
	return cc.read(out)
}

// WriteRequest encodes the provided *rpc.Response header and its associated
// body, writing both to the underlying connection using Msgpack encoding.
//
// When WriteRequest returns an error to net/rpc, it is propagated to the
// caller. This allows the caller to deal with the error unlike how
// WriteResponse has to close the Codec (and connection).
func (cc *MsgpackCodec) WriteRequest(r *rpc.Request, body interface{}) error {
	cc.writeLock.Lock()
	defer cc.writeLock.Unlock()
	if cc.closed {
		return io.EOF
	}
	if err := cc.enc.Encode(r); err != nil {
		return err
	}
	if err := cc.enc.Encode(body); err != nil {
		return err
	}
	if cc.bufW != nil {
		return cc.bufW.Flush()
	}
	return nil
}

func (cc *MsgpackCodec) Close() error {
	if cc.closed {
		return nil
	}
	cc.closed = true
	return cc.conn.Close()
}

func (cc *MsgpackCodec) read(obj interface{}) (err error) {
	if cc.closed {
		return io.EOF
	}

	// If nil is passed in, we should still attempt to read content to nowhere.
	if obj == nil {
		var obj2 interface{}
		return cc.dec.Decode(&obj2)
	}
	return cc.dec.Decode(obj)
}
