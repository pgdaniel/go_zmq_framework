// Package framework is a flow-based, language-agnostic runtime for
// blackbox nodes wired together by ZeroMQ pub/sub topics. See PROTOCOL.md
// for the wire contract and README.md for the tour.
package framework

/*
#cgo LDFLAGS: -lzmq
#include <zmq.h>
#include <stdlib.h>
*/
import "C"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"unsafe"
)

// Payload is a JSON object decoded with json.Number preserved (so a topic
// like "rpm" reads back as an integer, not silently promoted to float64).
type Payload = map[string]any

// Handler is the node contract: anything with HandleMessage can subscribe.
// Because it's a real Go interface, a node that forgets the method simply
// fails to satisfy Handler — a compile error at the Boot() call site, the
// Go-native equivalent of the Ruby original's runtime StrictContract raise.
type Handler interface {
	HandleMessage(topic string, payload Payload)
}

// HandlerFunc adapts a plain function to Handler, the way http.HandlerFunc
// adapts a function to http.Handler.
type HandlerFunc func(topic string, payload Payload)

func (f HandlerFunc) HandleMessage(topic string, payload Payload) { f(topic, payload) }

const pollTimeoutMS = 100 // how long the listener blocks in zmq_poll before looping

type opKind int

const (
	opSubscribe opKind = iota
	opDispatch
)

// busOp is the only thing ever sent to Bus.ops — subscribe and dispatch
// requests share one channel so they're strictly FIFO relative to each
// other. That matters: Subscribe() then Publish() from the same goroutine
// must be seen by the run loop in that order, or a message could be
// delivered before the subscription that should catch it exists.
type busOp struct {
	kind    opKind
	topic   string
	handler Handler
	payload []byte
}

// Bus is a ZeroMQ PUB/SUB transport. Every node owns one PUB socket (bound
// to its own port) and one SUB socket (connected to each peer, subscribed
// to everything, filtered on receipt). Wire format: a two-frame multipart
// message, [topic, json], per PROTOCOL.md.
//
// All subscriber state and handler dispatch is owned by a single goroutine
// (run), so handlers on one bus never execute concurrently with each
// other — matching the Ruby original's Monitor-guarded dispatch — without
// needing a recursive lock. A handler that calls Publish from within
// HandleMessage doesn't reenter anything: Publish just enqueues onto the
// same channel run() is reading, so the reentrant broadcast is handled
// once run() finishes the current message and loops around. That's a
// breadth-first delivery order rather than Ruby's synchronous nested call,
// which is the one observable behavioral difference from the original.
type Bus struct {
	ctx     unsafe.Pointer
	pubSock unsafe.Pointer
	subSock unsafe.Pointer

	// Port actually bound — differs from the requested port when 0
	// (OS-assigned ephemeral) was requested.
	Port int

	subscribers map[string][]Handler
	ops         chan busOp

	stopListener chan struct{}
	listenerDone chan struct{}
	closeOnce    sync.Once
}

// NewBus binds the PUB socket and connects the SUB socket to every peer.
// myPort may be 0 to bind an OS-assigned ephemeral port (read back via
// .Port). Peer entries are "host:port" or a full endpoint ("tcp://...").
// Pass bindHost "0.0.0.0" to accept peers from other machines.
func NewBus(myPort int, peers []string, bindHost string) (*Bus, error) {
	ctx := C.zmq_ctx_new()
	if ctx == nil {
		return nil, zmqError("ctx_new")
	}

	pubSock := C.zmq_socket(ctx, C.ZMQ_PUB)
	if pubSock == nil {
		return nil, zmqError("socket(PUB)")
	}
	bindEndpoint := fmt.Sprintf("tcp://%s:%s", bindHost, portToken(myPort))
	if rc := zmqBind(pubSock, bindEndpoint); rc != 0 {
		return nil, zmqError("bind to " + bindEndpoint)
	}

	subSock := C.zmq_socket(ctx, C.ZMQ_SUB)
	if subSock == nil {
		return nil, zmqError("socket(SUB)")
	}
	for _, peer := range peers {
		endpoint := peerEndpoint(peer)
		if rc := zmqConnect(subSock, endpoint); rc != 0 {
			return nil, zmqError("connect to " + endpoint)
		}
	}
	if rc := zmqSubscribeAll(subSock); rc != 0 {
		return nil, zmqError("subscribe")
	}

	port, err := readBoundPort(pubSock)
	if err != nil {
		return nil, err
	}

	b := &Bus{
		ctx:          ctx,
		pubSock:      pubSock,
		subSock:      subSock,
		Port:         port,
		subscribers:  make(map[string][]Handler),
		ops:          make(chan busOp, 1024),
		stopListener: make(chan struct{}),
		listenerDone: make(chan struct{}),
	}
	go b.run()
	go b.listenLoop()
	return b, nil
}

// Close stops the listener goroutine and releases both sockets and the
// context. Stop anything still publishing on this bus first (heartbeats,
// reader goroutines): publishing on a closed bus panics. Idempotent.
func (b *Bus) Close() {
	b.closeOnce.Do(func() {
		close(b.stopListener)
		<-b.listenerDone

		zero := C.int(0)
		C.zmq_setsockopt(b.subSock, C.ZMQ_LINGER, unsafe.Pointer(&zero), C.size_t(unsafe.Sizeof(zero)))
		C.zmq_setsockopt(b.pubSock, C.ZMQ_LINGER, unsafe.Pointer(&zero), C.size_t(unsafe.Sizeof(zero)))
		C.zmq_close(b.subSock)
		C.zmq_close(b.pubSock)
		C.zmq_ctx_term(b.ctx)

		close(b.ops)
	})
}

// Subscribe routes messages on topic to h.HandleMessage. Safe to call from
// any goroutine.
func (b *Bus) Subscribe(topic string, h Handler) {
	b.ops <- busOp{kind: opSubscribe, topic: topic, handler: h}
}

// Publish serializes payload as JSON and sends [topic, json] on the wire.
// A SUB socket never connects back to its own PUB, so Publish also
// enqueues local dispatch directly — otherwise two nodes sharing one bus
// in the same process could not hear each other, and in this framework
// every node's own heartbeat is exactly that case (it always subscribes
// to "heartbeat" and always publishes it).
func (b *Bus) Publish(topic string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// zmq_send returns the number of bytes sent on success, -1 on error —
	// unlike zmq_bind/zmq_connect/zmq_setsockopt, which return 0.
	if rc := zmqSendFrame(b.pubSock, []byte(topic), true); rc < 0 {
		return zmqError("publish topic")
	}
	if rc := zmqSendFrame(b.pubSock, raw, false); rc < 0 {
		return zmqError("publish payload")
	}

	b.ops <- busOp{kind: opDispatch, topic: topic, payload: raw}
	return nil
}

// run owns b.subscribers exclusively, so no lock is needed: every read and
// write happens on this one goroutine.
func (b *Bus) run() {
	for op := range b.ops {
		switch op.kind {
		case opSubscribe:
			b.subscribers[op.topic] = append(b.subscribers[op.topic], op.handler)
		case opDispatch:
			b.deliver(op.topic, op.payload)
		}
	}
}

// A bad payload or a panicking subscriber must never kill this goroutine:
// one poisoned message would otherwise leave the node deaf for good while
// its heartbeat keeps reporting "ok".
func (b *Bus) deliver(topic string, raw []byte) {
	subs := b.subscribers[topic]
	if len(subs) == 0 {
		return
	}

	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var payload Payload
	if err := dec.Decode(&payload); err != nil {
		fmt.Fprintf(os.Stderr, "[Framework Error] Dropping malformed payload on %s: %v\n", topic, err)
		return
	}

	// Copy so a handler that subscribes mid-dispatch can't mutate the
	// slice we're iterating.
	handlers := append([]Handler(nil), subs...)
	for _, h := range handlers {
		invoke(h, topic, payload)
	}
}

func invoke(h Handler, topic string, payload Payload) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "[Framework Error] %T failed handling %s: %v\n", h, topic, r)
		}
	}()
	h.HandleMessage(topic, payload)
}

func (b *Bus) listenLoop() {
	defer close(b.listenerDone)

	var item C.zmq_pollitem_t
	item.socket = b.subSock
	item.events = C.ZMQ_POLLIN

	for {
		select {
		case <-b.stopListener:
			return
		default:
		}

		item.revents = 0
		rc := C.zmq_poll(&item, 1, C.long(pollTimeoutMS))
		if rc == -1 {
			fmt.Fprintln(os.Stderr, "[Framework Error] Listener poll failed")
			continue
		}
		if rc == 0 || item.revents&C.ZMQ_POLLIN == 0 {
			continue
		}

		topic, payload, ok := recvFrames(b.subSock)
		if !ok {
			continue
		}
		b.ops <- busOp{kind: opDispatch, topic: topic, payload: payload}
	}
}

func recvFrames(sock unsafe.Pointer) (topic string, payload []byte, ok bool) {
	var topicMsg C.zmq_msg_t
	C.zmq_msg_init(&topicMsg)
	if C.zmq_msg_recv(&topicMsg, sock, 0) == -1 {
		C.zmq_msg_close(&topicMsg)
		return "", nil, false
	}

	var more C.int
	moreSize := C.size_t(unsafe.Sizeof(more))
	C.zmq_getsockopt(sock, C.ZMQ_RCVMORE, unsafe.Pointer(&more), &moreSize)

	topic = C.GoStringN((*C.char)(C.zmq_msg_data(&topicMsg)), C.int(C.zmq_msg_size(&topicMsg)))
	C.zmq_msg_close(&topicMsg)

	// Drop anything that doesn't match the two-frame [topic, json] wire format.
	if more == 0 {
		return "", nil, false
	}

	var payloadMsg C.zmq_msg_t
	C.zmq_msg_init(&payloadMsg)
	if C.zmq_msg_recv(&payloadMsg, sock, 0) == -1 {
		C.zmq_msg_close(&payloadMsg)
		return "", nil, false
	}
	payload = C.GoBytes(C.zmq_msg_data(&payloadMsg), C.int(C.zmq_msg_size(&payloadMsg)))
	C.zmq_msg_close(&payloadMsg)

	return topic, payload, true
}

func zmqSendFrame(sock unsafe.Pointer, data []byte, more bool) C.int {
	flags := C.int(0)
	if more {
		flags = C.ZMQ_SNDMORE
	}
	if len(data) == 0 {
		return C.zmq_send(sock, unsafe.Pointer(nil), 0, flags)
	}
	return C.zmq_send(sock, unsafe.Pointer(&data[0]), C.size_t(len(data)), flags)
}

func zmqBind(sock unsafe.Pointer, endpoint string) C.int {
	cs := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cs))
	return C.zmq_bind(sock, cs)
}

func zmqConnect(sock unsafe.Pointer, endpoint string) C.int {
	cs := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cs))
	return C.zmq_connect(sock, cs)
}

func zmqSubscribeAll(sock unsafe.Pointer) C.int {
	empty := C.CString("")
	defer C.free(unsafe.Pointer(empty))
	return C.zmq_setsockopt(sock, C.ZMQ_SUBSCRIBE, unsafe.Pointer(empty), 0)
}

func readBoundPort(sock unsafe.Pointer) (int, error) {
	buf := make([]byte, 256)
	size := C.size_t(len(buf))
	if C.zmq_getsockopt(sock, C.ZMQ_LAST_ENDPOINT, unsafe.Pointer(&buf[0]), &size) != 0 {
		return 0, zmqError("getsockopt LAST_ENDPOINT")
	}
	endpoint := strings.TrimRight(string(buf[:size]), "\x00")
	idx := strings.LastIndex(endpoint, ":")
	if idx == -1 {
		return 0, fmt.Errorf("[Framework Error] unparseable bound endpoint %q", endpoint)
	}
	return strconv.Atoi(endpoint[idx+1:])
}

func peerEndpoint(peer string) string {
	if strings.Contains(peer, "://") {
		return peer
	}
	return "tcp://" + peer
}

func portToken(port int) string {
	if port == 0 {
		return "*"
	}
	return strconv.Itoa(port)
}

func zmqError(action string) error {
	msg := C.GoString(C.zmq_strerror(C.zmq_errno()))
	return fmt.Errorf("[Framework Error] ZeroMQ %s failed: %s", action, msg)
}
