package framework

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// Reads raw frames off a real SocketCAN interface (can0, vcan0, ...) and
// rebroadcasts each one onto the ZeroMQ bus. Talks to the kernel directly
// via raw sockets + a couple of ioctl/struct calls (see linux/can.h,
// linux/sockios.h) rather than a third-party module, the same approach the
// Ruby and Zig versions take.
//
// Classic CAN only: reads assume the 16-byte struct can_frame. That is
// safe even on an FD-enabled interface — the kernel only delivers 72-byte
// canfd_frames to sockets that opt in via CAN_RAW_FD_FRAMES, which this
// one never does — but it means FD traffic is invisible here.

const (
	afCAN        = 29
	canRAW       = 1
	frameSize    = 16 // sizeof(struct can_frame): 4(id) + 1(len) + 3(pad) + 8(data)
	canEFFFlag   = 0x80000000
	canEFFMask   = 0x1FFFFFFF
	canSFFMask   = 0x7FF
	sizeofIfreq  = 40
	siocgifindex = 0x8933
)

type CanFrame struct {
	ID       uint32
	Extended bool
	Dlc      uint8
	Data     []byte
}

func ParseCanFrame(raw []byte) CanFrame {
	idRaw := binary.LittleEndian.Uint32(raw[0:4])
	length := raw[4]
	if length > 8 {
		length = 8
	}
	extended := idRaw&canEFFFlag != 0
	mask := uint32(canSFFMask)
	if extended {
		mask = canEFFMask
	}

	data := make([]byte, length)
	copy(data, raw[8:8+length])
	return CanFrame{ID: idRaw & mask, Extended: extended, Dlc: length, Data: data}
}

// CanBridge is a pure producer: it has no interest in bus traffic, so
// HandleMessage is a no-op that only exists to satisfy the node contract.
type CanBridge struct {
	bus   *Bus
	topic string
	fd    int

	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

// NewCanBridge opens the CAN socket and starts the reader goroutine. Fails
// fast (exits the process) if the interface doesn't exist, matching the
// Ruby/Zig versions' fail-fast-with-the-underlying-errno behavior.
func NewCanBridge(bus *Bus, iface, topic string) *CanBridge {
	fd, err := openCanSocket(iface)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[Framework Error] CanBridge failed to open %s: %v\n", iface, err)
		os.Exit(1)
	}

	cb := &CanBridge{bus: bus, topic: topic, fd: fd, stopCh: make(chan struct{}), doneCh: make(chan struct{})}
	go cb.readLoop()
	return cb
}

func (cb *CanBridge) HandleMessage(topic string, payload Payload) {}

// Close stops the reader goroutine and closes the CAN socket. Closing the
// socket from here is what interrupts the reader's blocking read.
func (cb *CanBridge) Close() {
	cb.stopOnce.Do(func() {
		close(cb.stopCh)
		syscall.Close(cb.fd)
		<-cb.doneCh
	})
}

func (cb *CanBridge) readLoop() {
	defer close(cb.doneCh)

	buf := make([]byte, frameSize)
	for {
		n, err := syscall.Read(cb.fd, buf)
		if err != nil {
			select {
			case <-cb.stopCh:
				return // Close() interrupted the blocking read
			default:
			}
			fmt.Fprintf(os.Stderr, "[Framework Error] CanBridge read failed: %v\n", err)
			continue
		}
		if n != frameSize {
			continue
		}

		frame := ParseCanFrame(buf)
		// encoding/json base64-encodes []byte by default, which isn't the
		// wire format PROTOCOL.md promises (a plain array of small ints),
		// so convert explicitly.
		data := make([]int, len(frame.Data))
		for i, b := range frame.Data {
			data[i] = int(b)
		}

		err = cb.bus.Publish(cb.topic, map[string]any{
			"id": frame.ID, "extended": frame.Extended, "dlc": frame.Dlc, "data": data,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Framework Error] CanBridge publish failed: %v\n", err)
		}
	}
}

func openCanSocket(iface string) (int, error) {
	fd, err := syscall.Socket(afCAN, syscall.SOCK_RAW, canRAW)
	if err != nil {
		return -1, err
	}

	ifindex, err := interfaceIndex(fd, iface)
	if err != nil {
		syscall.Close(fd)
		return -1, err
	}

	// struct sockaddr_can { sa_family_t can_family; int can_ifindex; ... 8 more bytes ... };
	addr := make([]byte, 16)
	binary.LittleEndian.PutUint16(addr[0:2], uint16(afCAN))
	binary.LittleEndian.PutUint32(addr[4:8], uint32(ifindex))

	_, _, errno := syscall.RawSyscall(syscall.SYS_BIND, uintptr(fd), uintptr(unsafe.Pointer(&addr[0])), uintptr(len(addr)))
	if errno != 0 {
		syscall.Close(fd)
		return -1, errno
	}
	return fd, nil
}

func interfaceIndex(fd int, iface string) (int32, error) {
	var ifr [sizeofIfreq]byte
	copy(ifr[0:16], iface)

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), siocgifindex, uintptr(unsafe.Pointer(&ifr[0])))
	if errno != 0 {
		return 0, errno
	}
	return int32(binary.LittleEndian.Uint32(ifr[16:20])), nil
}
