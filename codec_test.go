// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MIT

package msgpackrpc

import (
	"errors"
	"io"
	"net"
	"net/rpc"
	"testing"
)

// TestMsgpackCodec_RequestResponse verifies that a request sent from a client
// is correctly read by the server and that a response sent from the server is
// correctly read by the client. It tests both WriteRequest and WriteResponse
// (both of which call the internal write() function).
func TestMsgpackCodec_RequestResponse(t *testing.T) {
	// Use a table test to try different buffering configurations.
	tests := []struct {
		name      string
		bufReads  bool
		bufWrites bool
	}{
		{"BufferedReadWrite", true, true},
		{"UnbufferedReadWrite", false, false},
		{"BufferedReadOnly", true, false},
		{"BufferedWriteOnly", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a pair of connected endpoints.
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()

			// Create client and server codecs using the same msgpack handle.
			clientCodec := NewCodecFromHandle(tt.bufReads, tt.bufWrites, clientConn, msgpackHandle)
			serverCodec := NewCodecFromHandle(tt.bufReads, tt.bufWrites, serverConn, msgpackHandle)

			// --- Test Request Path ---

			// Prepare a test RPC request.
			req := &rpc.Request{
				ServiceMethod: "TestService.TestMethod",
				Seq:           1,
			}
			reqBody := "request payload"

			// Start a goroutine to write the request from the client.
			reqWriteDone := make(chan error, 1)
			go func() {
				err := clientCodec.WriteRequest(req, reqBody)
				reqWriteDone <- err
			}()

			// On the server side, read the header and body.
			var readReq rpc.Request
			if err := serverCodec.ReadRequestHeader(&readReq); err != nil {
				t.Fatalf("ReadRequestHeader failed: %v", err)
			}
			var readReqBody string
			if err := serverCodec.ReadRequestBody(&readReqBody); err != nil {
				t.Fatalf("ReadRequestBody failed: %v", err)
			}
			if readReq.ServiceMethod != req.ServiceMethod || readReq.Seq != req.Seq {
				t.Errorf("Request header mismatch: got %+v, want %+v", readReq, req)
			}
			if readReqBody != reqBody {
				t.Errorf("Request body mismatch: got %q, want %q", readReqBody, reqBody)
			}

			// Make sure the client's write completed without error.
			if err := <-reqWriteDone; err != nil {
				t.Fatalf("WriteRequest failed: %v", err)
			}

			// --- Test Response Path ---

			// Prepare a test RPC response.
			resp := &rpc.Response{
				ServiceMethod: "TestService.TestMethod",
				Seq:           1,
			}
			respBody := "response payload"

			// Start a goroutine to write the response from the server.
			respWriteDone := make(chan error, 1)
			go func() {
				err := serverCodec.WriteResponse(resp, respBody)
				respWriteDone <- err
			}()

			// On the client side, read the response header and body.
			var readResp rpc.Response
			if err := clientCodec.ReadResponseHeader(&readResp); err != nil {
				t.Fatalf("ReadResponseHeader failed: %v", err)
			}
			var readRespBody string
			if err := clientCodec.ReadResponseBody(&readRespBody); err != nil {
				t.Fatalf("ReadResponseBody failed: %v", err)
			}
			if readResp.ServiceMethod != resp.ServiceMethod || readResp.Seq != resp.Seq {
				t.Errorf("Response header mismatch: got %+v, want %+v", readResp, resp)
			}
			if readRespBody != respBody {
				t.Errorf("Response body mismatch: got %q, want %q", readRespBody, respBody)
			}

			// Ensure the server's write completed successfully.
			if err := <-respWriteDone; err != nil {
				t.Fatalf("WriteResponse failed: %v", err)
			}
		})
	}
}

// TestMsgpackCodec_NilBody verifies that if a nil value is provided for the
// request body then the codec still performs a decode on the wire. (The
// implementation of read always attempts to decode even when obj == nil.)
func TestMsgpackCodec_NilBody(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	clientCodec := NewCodec(true, true, clientConn)
	serverCodec := NewCodec(true, true, serverConn)

	req := &rpc.Request{
		ServiceMethod: "TestService.NilMethod",
		Seq:           2,
	}

	// Write a request with a nil body.
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- clientCodec.WriteRequest(req, nil)
	}()

	// Read the header normally.
	var readReq rpc.Request
	if err := serverCodec.ReadRequestHeader(&readReq); err != nil {
		t.Fatalf("ReadRequestHeader failed: %v", err)
	}
	// Now read the body; even though we passed nil on the write side, the codec
	// should attempt to decode the nil value.
	if err := serverCodec.ReadRequestBody(nil); err != nil {
		t.Fatalf("ReadRequestBody (with nil) failed: %v", err)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("WriteRequest (with nil body) failed: %v", err)
	}
}

// TestMsgpackCodec_Close ensures that once the codec is closed, subsequent
// reads or writes immediately return io.EOF.
func TestMsgpackCodec_Close(t *testing.T) {
	clientConn, _ := net.Pipe()
	codec := NewCodec(true, true, clientConn)

	// Close the codec.
	if err := codec.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Further write operations should return io.EOF.
	if err := codec.WriteRequest(&rpc.Request{}, "test"); err != io.EOF {
		t.Errorf("WriteRequest after Close: got %v, want io.EOF", err)
	}

	// Further read operations should also return io.EOF.
	if err := codec.ReadResponseHeader(&rpc.Response{}); err != io.EOF {
		t.Errorf("ReadResponseHeader after Close: got %v, want io.EOF", err)
	}
}

// failingConn is an io.ReadWriteCloser that simulates a write error and records when it’s closed.
type failingConn struct {
	closed bool
}

func (fc *failingConn) Read(p []byte) (n int, err error) {
	return 0, io.EOF
}

func (fc *failingConn) Write(p []byte) (n int, err error) {
	return 0, errors.New("simulated write error")
}

func (fc *failingConn) Close() error {
	fc.closed = true
	return nil
}

// TestWriteResponse_ErrorClosesConn ensures that if an error occurs during WriteResponse,
// the underlying connection is closed.
func TestWriteResponse_ErrorClosesConn(t *testing.T) {
	// Create a connection that always fails on Write.
	fc := &failingConn{}
	// Use buffering (so that cc.bufW is non-nil) to force a flush.
	codec := NewCodec(true, true, fc)

	// Prepare a dummy response.
	resp := &rpc.Response{
		ServiceMethod: "Test.Method",
		Seq:           42,
	}

	// Invoke WriteResponse; since our connection always errors on Write,
	// we expect an error.
	err := codec.WriteResponse(resp, "test payload")
	if err == nil {
		t.Fatal("expected error from WriteResponse, got nil")
	}

	// Our intended behavior is that a WriteResponse error causes the connection to be closed.
	if !fc.closed {
		t.Error("expected connection to be closed after WriteResponse error, but it wasn't")
	}

	// With the connection closed, further writes should immediately return io.EOF.
	err = codec.WriteResponse(resp, "test payload")
	if err != io.EOF {
		t.Errorf("expected io.EOF after connection closed, got: %v", err)
	}
}
