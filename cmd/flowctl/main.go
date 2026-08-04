// Command flowctl runs a flow manifest: assigns each node a free port,
// computes the peer wiring, spawns every node with that wiring in its
// environment, and streams their output with a [name] prefix. Ctrl-C
// stops everything.
//
//	flowctl              # runs ./flow.yml
//	flowctl other.yml
//	flowctl --plan       # print computed wiring, run nothing
//	flowctl --graph      # print the topology as JSON, run nothing
//	flowctl --live :5656 # run flow and serve SSE stream on :5656
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"go_zmq_framework/framework"
)

type exited struct {
	name string
	err  error
}

func main() {
	planOnly := flag.Bool("plan", false, "Print the computed wiring and exit")
	graphOnly := flag.Bool("graph", false, "Print the node topology as JSON and exit")
	liveAddr := flag.String("live", "", "Serve SSE stream on this address (e.g. :5656)")
	flag.Parse()

	manifest := "flow.yml"
	if flag.NArg() > 0 {
		manifest = flag.Arg(0)
	}
	manifestPath, err := filepath.Abs(manifest)
	if err != nil {
		fatal(err)
	}
	root := filepath.Dir(manifestPath)

	flow, err := framework.LoadFlow(manifestPath)
	if err != nil {
		fatal(err)
	}

	if *graphOnly {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(flow.Graph()); err != nil {
			fatal(err)
		}
		return
	}

	ports := make(map[string]int, len(flow.Nodes))
	for _, node := range flow.Nodes {
		port, err := freePort()
		if err != nil {
			fatal(err)
		}
		ports[node.Name] = port
	}

	wiring, err := flow.Wiring(ports)
	if err != nil {
		fatal(err)
	}

	if *planOnly {
		for _, entry := range wiring {
			fmt.Println(entry.NodeName)
			for _, pair := range entry.Env {
				fmt.Printf("  %s=%s\n", pair.Key, pair.Value)
			}
		}
		return
	}

	wiringByName := make(map[string][]framework.EnvPair, len(wiring))
	for _, entry := range wiring {
		wiringByName[entry.NodeName] = entry.Env
	}

	var printLock sync.Mutex
	type child struct {
		name string
		cmd  *exec.Cmd
	}
	var children []child

	exitCh := make(chan exited, len(flow.Nodes))

	for _, node := range flow.Nodes {
		env := append(os.Environ(), envStrings(wiringByName[node.Name])...)

		cmd := exec.Command("sh", "-c", node.Cmd)
		cmd.Dir = root
		cmd.Env = env

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			fatal(err)
		}
		stderr, err := cmd.StderrPipe()
		if err != nil {
			fatal(err)
		}
		if err := cmd.Start(); err != nil {
			fatal(err)
		}

		children = append(children, child{name: node.Name, cmd: cmd})
		go pump(stdout, node.Name, &printLock)
		go pump(stderr, node.Name, &printLock)
		go func(name string, cmd *exec.Cmd) {
			exitCh <- exited{name: name, err: cmd.Wait()}
		}(node.Name, cmd)
	}

	names := make([]string, len(children))
	for i, c := range children {
		names[i] = c.name
	}
	fmt.Printf("[flowctl] started %d nodes: %s\n", len(children), joinNames(names))

	// Start live SSE server if requested
	var liveStop func()
	if *liveAddr != "" {
		peers := make([]string, 0, len(ports))
		for _, port := range ports {
			peers = append(peers, fmt.Sprintf("127.0.0.1:%d", port))
		}

		clients := make(map[chan []byte]struct{})
		var clientsMu sync.Mutex

		liveStop, err = framework.TapAll(peers, func(topic string, payload []byte) {
			msg := map[string]any{"topic": topic, "payload": json.RawMessage(payload)}
			data, _ := json.Marshal(msg)
			clientsMu.Lock()
			defer clientsMu.Unlock()
			for ch := range clients {
				select {
				case ch <- data:
				default:
					// Slow client, drop message
				}
			}
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "[flowctl] failed to start tap: %v\n", err)
		} else {
			mux := http.NewServeMux()

			mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header().Set("Cache-Control", "no-cache")
				w.Header().Set("Connection", "keep-alive")
				w.Header().Set("Access-Control-Allow-Origin", "*")

				flusher, ok := w.(http.Flusher)
				if !ok {
					http.Error(w, "Streaming not supported", http.StatusInternalServerError)
					return
				}

				ch := make(chan []byte, 64)
				clientsMu.Lock()
				clients[ch] = struct{}{}
				clientsMu.Unlock()

				defer func() {
					clientsMu.Lock()
					delete(clients, ch)
					clientsMu.Unlock()
				}()

				for {
					select {
					case data := <-ch:
						fmt.Fprintf(w, "data: %s\n\n", data)
						flusher.Flush()
					case <-r.Context().Done():
						return
					}
				}
			})

			mux.HandleFunc("/graph", func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Access-Control-Allow-Origin", "*")
				json.NewEncoder(w).Encode(flow.Graph())
			})

			go func() {
				fmt.Printf("[flowctl] live server listening on %s\n", *liveAddr)
				if err := http.ListenAndServe(*liveAddr, mux); err != nil {
					fmt.Fprintf(os.Stderr, "[flowctl] live server error: %v\n", err)
				}
			}()
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	remaining := len(children)
	for remaining > 0 {
		select {
		case e := <-exitCh:
			fmt.Printf("[flowctl] %s exited: %s\n", e.name, exitDescription(e.err))
			remaining--

		case <-sigCh:
			fmt.Printf("\n[flowctl] shutting down %d nodes\n", remaining)
			if liveStop != nil {
				liveStop()
			}
			for _, c := range children {
				if c.cmd.Process != nil {
					_ = c.cmd.Process.Signal(syscall.SIGTERM)
				}
			}
			drainExits(exitCh, remaining, 3*time.Second)
			fmt.Println("[flowctl] all nodes exited")
			return
		}
	}
	if liveStop != nil {
		liveStop()
	}
	fmt.Println("[flowctl] all nodes exited")
}

// drainExits waits (briefly) for already-signaled children to actually
// exit, so flowctl doesn't return before they've had a chance to run
// their own TERM handlers — but it's a courtesy, not a guarantee, so it
// gives up after timeout rather than hanging flowctl on a stuck child.
func drainExits(exitCh <-chan exited, remaining int, timeout time.Duration) {
	deadline := time.After(timeout)
	for remaining > 0 {
		select {
		case <-exitCh:
			remaining--
		case <-deadline:
			return
		}
	}
}

func envStrings(pairs []framework.EnvPair) []string {
	out := make([]string, len(pairs))
	for i, p := range pairs {
		out[i] = p.Key + "=" + p.Value
	}
	return out
}

func joinNames(names []string) string {
	out := ""
	for i, n := range names {
		if i != 0 {
			out += ", "
		}
		out += n
	}
	return out
}

func pump(r io.Reader, name string, lock *sync.Mutex) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		lock.Lock()
		fmt.Printf("[%s] %s\n", name, scanner.Text())
		lock.Unlock()
	}
}

func exitDescription(err error) string {
	if err == nil {
		return "status 0"
	}
	var exitErr *exec.ExitError
	if ok := asExitError(err, &exitErr); ok {
		return fmt.Sprintf("status %d", exitErr.ExitCode())
	}
	return err.Error()
}

func asExitError(err error, target **exec.ExitError) bool {
	if e, ok := err.(*exec.ExitError); ok {
		*target = e
		return true
	}
	return false
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
