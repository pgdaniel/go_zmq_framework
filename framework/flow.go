package framework

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Flow parses a flow manifest (flow.yml) — the graph as data, Node-RED
// style. The manifest is the ONLY place that knows the topology; node
// processes learn their wiring from environment variables computed here
// (see Wiring), and a node's code never mentions another node.
//
// Only the small subset of YAML flow.yml actually uses is supported: a
// top-level `nodes:` map, one 2-space-indented block per node, each with
// `cmd:` (scalar), `publishes:`/`subscribes:` (flow lists `[a, b]`), and
// an optional `env:` (flow map `{ K: "v" }`). That's deliberate — the
// whole point of the manifest is to stay this simple, and it means this
// package has no YAML dependency at all.
type EnvPair struct {
	Key   string
	Value string
}

type Node struct {
	Name       string
	Cmd        string
	Publishes  []string
	Subscribes []string
	Env        []EnvPair
}

type WiringEntry struct {
	NodeName string
	Env      []EnvPair
}

type Flow struct {
	Nodes []Node
}

func LoadFlow(path string) (*Flow, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseFlow(string(data))
}

func ParseFlow(text string) (*Flow, error) {
	nodes, err := parseNodes(text)
	if err != nil {
		return nil, err
	}
	f := &Flow{Nodes: nodes}
	f.warnAboutDeafSubscriptions()
	return f, nil
}

// Wiring computes the environment for every node process, given a
// {name => port} map. This is the whole trick that keeps nodes
// blackboxes: each node's peer list is computed from who publishes the
// topics it subscribes to.
func (f *Flow) Wiring(ports map[string]int) ([]WiringEntry, error) {
	out := make([]WiringEntry, 0, len(f.Nodes))
	for _, node := range f.Nodes {
		peers := f.peerNames(node)

		peerStrs := make([]string, len(peers))
		for i, name := range peers {
			port, ok := ports[name]
			if !ok {
				return nil, fmt.Errorf("[Framework Error] no port assigned for %s", name)
			}
			peerStrs[i] = fmt.Sprintf("127.0.0.1:%d", port)
		}

		myPort, ok := ports[node.Name]
		if !ok {
			return nil, fmt.Errorf("[Framework Error] no port assigned for %s", node.Name)
		}

		env := []EnvPair{
			{Key: "BUS_PORT", Value: strconv.Itoa(myPort)},
			{Key: "BUS_PEERS", Value: strings.Join(peerStrs, ",")},
			{Key: "BUS_SUBSCRIBES", Value: strings.Join(node.Subscribes, ",")},
			{Key: "NODE_NAME", Value: node.Name},
		}
		env = append(env, node.Env...)

		out = append(out, WiringEntry{NodeName: node.Name, Env: env})
	}
	return out, nil
}

// Graph JSON shapes, for flowctl --graph.
type GraphNode struct {
	Name       string            `json:"name"`
	Cmd        string            `json:"cmd"`
	Publishes  []string          `json:"publishes"`
	Subscribes []string          `json:"subscribes"`
	Env        map[string]string `json:"env"`
}

type GraphEdge struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Topic string `json:"topic"`
}

type GraphUnresolved struct {
	Topic string `json:"topic"`
	To    string `json:"to"`
}

type Graph struct {
	Nodes      []GraphNode       `json:"nodes"`
	Edges      []GraphEdge       `json:"edges"`
	Unresolved []GraphUnresolved `json:"unresolved"`
}

// Graph is the node-to-node topology, for visualization. Every topic a
// node subscribes to becomes an edge from each of its publishers, except
// heartbeat (implicit, all-to-all) and topics nobody publishes (surfaced
// as Unresolved instead of a dangling edge).
func (f *Flow) Graph() Graph {
	g := Graph{Nodes: make([]GraphNode, 0, len(f.Nodes))}

	for _, n := range f.Nodes {
		env := make(map[string]string, len(n.Env))
		for _, pair := range n.Env {
			env[pair.Key] = pair.Value
		}
		g.Nodes = append(g.Nodes, GraphNode{
			Name: n.Name, Cmd: n.Cmd, Publishes: n.Publishes, Subscribes: n.Subscribes, Env: env,
		})
	}

	for _, node := range f.Nodes {
		for _, topic := range node.Subscribes {
			if topic == "heartbeat" {
				continue
			}
			publishers := f.publisherNames(topic, node.Name)
			if len(publishers) == 0 {
				g.Unresolved = append(g.Unresolved, GraphUnresolved{Topic: topic, To: node.Name})
				continue
			}
			for _, from := range publishers {
				g.Edges = append(g.Edges, GraphEdge{From: from, To: node.Name, Topic: topic})
			}
		}
	}

	return g
}

// Every node broadcasts :heartbeat implicitly, so for that topic everyone
// counts as a publisher. A node never peers with itself — the bus already
// delivers its own publishes locally.
func (f *Flow) publisherNames(topic, exclude string) []string {
	var out []string
	if topic == "heartbeat" {
		for _, n := range f.Nodes {
			if n.Name != exclude {
				out = append(out, n.Name)
			}
		}
		return out
	}
	for _, n := range f.Nodes {
		if n.Name == exclude {
			continue
		}
		for _, p := range n.Publishes {
			if p == topic {
				out = append(out, n.Name)
				break
			}
		}
	}
	return out
}

func (f *Flow) peerNames(node Node) []string {
	seen := make(map[string]bool)
	var out []string
	for _, topic := range node.Subscribes {
		for _, name := range f.publisherNames(topic, node.Name) {
			if !seen[name] {
				seen[name] = true
				out = append(out, name)
			}
		}
	}
	return out
}

func (f *Flow) warnAboutDeafSubscriptions() {
	for _, node := range f.Nodes {
	outer:
		for _, topic := range node.Subscribes {
			if topic == "heartbeat" {
				continue
			}
			for _, n := range f.Nodes {
				for _, p := range n.Publishes {
					if p == topic {
						continue outer
					}
				}
			}
			fmt.Fprintf(os.Stderr,
				"[Framework Warning] %s subscribes to %q but no node in the flow publishes it\n",
				node.Name, topic)
		}
	}
}

func stripQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

func indentOf(line string) int {
	n := 0
	for n < len(line) && line[n] == ' ' {
		n++
	}
	return n
}

func isBlankOrComment(trimmed string) bool {
	return trimmed == "" || trimmed[0] == '#'
}

// splitKeyValue splits "key: value" (or "key:" with an empty value) at the
// first colon-then-space (or a trailing colon). Values may contain their
// own colons (e.g. a URL) without being mistaken for a new key.
func splitKeyValue(trimmed string) (key, value string, ok bool) {
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] != ':' {
			continue
		}
		if i+1 == len(trimmed) {
			return trimmed[:i], "", true
		}
		if trimmed[i+1] == ' ' {
			return trimmed[:i], strings.TrimSpace(trimmed[i+1:]), true
		}
	}
	return "", "", false
}

func parseFlowList(value string) []string {
	if len(value) < 2 || value[0] != '[' || value[len(value)-1] != ']' {
		return nil
	}
	inner := value[1 : len(value)-1]
	var out []string
	for _, part := range strings.Split(inner, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			out = append(out, stripQuotes(trimmed))
		}
	}
	return out
}

func parseFlowMap(value string) []EnvPair {
	if len(value) < 2 || value[0] != '{' || value[len(value)-1] != '}' {
		return nil
	}
	inner := value[1 : len(value)-1]
	var out []EnvPair
	for _, part := range strings.Split(inner, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		k, v, ok := splitKeyValue(trimmed)
		if !ok {
			continue
		}
		out = append(out, EnvPair{Key: strings.TrimSpace(k), Value: stripQuotes(v)})
	}
	return out
}

type building struct {
	name       string
	cmd        string
	hasCmd     bool
	publishes  []string
	subscribes []string
	env        []EnvPair
}

func finishNode(out *[]Node, b building) error {
	if !b.hasCmd {
		return fmt.Errorf("[Framework Error] Flow node %s needs a cmd", b.name)
	}
	*out = append(*out, Node{
		Name: b.name, Cmd: b.cmd, Publishes: b.publishes, Subscribes: b.subscribes, Env: b.env,
	})
	return nil
}

func parseNodes(text string) ([]Node, error) {
	lines := strings.Split(text, "\n")

	// Find the top-level `nodes:` key.
	lineIdx := 0
	foundNodesKey := false
	for ; lineIdx < len(lines); lineIdx++ {
		line := strings.TrimRight(lines[lineIdx], "\r")
		trimmed := strings.TrimSpace(line)
		if indentOf(line) != 0 {
			continue
		}
		if isBlankOrComment(trimmed) {
			continue
		}
		if trimmed == "nodes:" {
			foundNodesKey = true
			lineIdx++
			break
		}
		// Some other top-level key before `nodes:` — keep scanning.
	}
	if !foundNodesKey {
		return nil, fmt.Errorf("[Framework Error] Flow manifest needs a top-level \"nodes\" map")
	}

	var out []Node
	nodeIndent := -1
	fieldIndent := -1
	var current *building

	for ; lineIdx < len(lines); lineIdx++ {
		line := strings.TrimRight(lines[lineIdx], "\r")
		trimmed := strings.TrimSpace(line)
		if isBlankOrComment(trimmed) {
			continue
		}

		indent := indentOf(line)
		if indent == 0 {
			break // next top-level key: nodes section is over
		}
		if nodeIndent == -1 {
			nodeIndent = indent
		}

		if indent == nodeIndent {
			if current != nil {
				if err := finishNode(&out, *current); err != nil {
					return nil, err
				}
			}
			name := trimmed
			if strings.HasSuffix(name, ":") {
				name = name[:len(name)-1]
			}
			current = &building{name: name}
			fieldIndent = -1
			continue
		}

		if fieldIndent == -1 {
			fieldIndent = indent
		}
		if indent != fieldIndent || current == nil {
			continue
		}

		key, value, ok := splitKeyValue(trimmed)
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		switch key {
		case "cmd":
			current.cmd = stripQuotes(value)
			current.hasCmd = true
		case "publishes":
			current.publishes = parseFlowList(value)
		case "subscribes":
			current.subscribes = parseFlowList(value)
		case "env":
			current.env = parseFlowMap(value)
		}
	}
	if current != nil {
		if err := finishNode(&out, *current); err != nil {
			return nil, err
		}
	}

	return out, nil
}
