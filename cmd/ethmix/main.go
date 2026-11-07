// Command ethmix: Ethereum / EVM JSON-RPC over the Nym mixnet.
//
//	ethmix nym setup               start nym-socks5-client on a random active exit gateway
//	ethmix serve                   local RPC gateway: http://127.0.0.1:8545/<chain> -> mixnet -> public RPC
//	ethmix call|balance|block|send talk to a public RPC directly over the mixnet
//	ethmix probe                   which public RPCs answer Nym exit traffic, and how fast
//	ethmix ip                      which IP do RPC providers see? (a Nym exit gateway)
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/apeirs/ethmix/gateway"
	"github.com/apeirs/ethmix/mixnet"
	"github.com/apeirs/ethmix/rpc"
)

const version = "0.1.0"

var start = time.Now()

func tracef(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[%7.2fs] "+format+"\n", append([]any{time.Since(start).Seconds()}, args...)...)
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s "+format+"\n", append([]any{time.Now().Format("15:04:05")}, args...)...)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "ethmix:", err)
	os.Exit(1)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

type common struct {
	proxy, dns, chain, rpcURL string
	verbose, jsonOut          bool
	timeout                   time.Duration
}

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.proxy, "proxy", envOr("ETHMIX_PROXY", "127.0.0.1:1080"), "nym-socks5-client SOCKS5 address ($ETHMIX_PROXY)")
	fs.StringVar(&c.dns, "dns", envOr("ETHMIX_DNS", mixnet.DNSDoH), "name resolution: doh (DoH over the mixnet, connect by IPv4) or exit")
	fs.StringVar(&c.chain, "chain", "ethereum", "network: ethereum base arbitrum optimism polygon bnb gnosis sepolia")
	fs.StringVar(&c.rpcURL, "rpc", "", "use this https RPC URL instead of the built-in list")
	fs.BoolVar(&c.verbose, "v", false, "trace every mixnet step to stderr")
	fs.BoolVar(&c.jsonOut, "json", false, "JSON output")
	fs.DurationVar(&c.timeout, "timeout", 5*time.Minute, "overall timeout for the command")
}

func (c *common) dialer() *mixnet.Dialer {
	proxy, err := mixnet.ParseProxy(c.proxy)
	if err != nil {
		die(err)
	}
	if c.dns != mixnet.DNSDoH && c.dns != mixnet.DNSExit {
		die(fmt.Errorf("--dns must be doh or exit"))
	}
	d := &mixnet.Dialer{Proxy: proxy, DNS: c.dns}
	if c.verbose {
		d.Trace = tracef
	}
	return d
}

func (c *common) client() *rpc.Client {
	cl := &rpc.Client{D: c.dialer()}
	if c.verbose {
		cl.Log = tracef
	}
	return cl
}

func (c *common) urls() []string {
	if c.rpcURL != "" {
		return []string{c.rpcURL}
	}
	ch, ok := rpc.ByName(c.chain)
	if !ok {
		die(fmt.Errorf("unknown chain %q (see `ethmix chains`), or pass --rpc URL", c.chain))
	}
	return ch.URLs
}

func (c *common) ctx() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	return ctx, func() { stop(); cancel() }
}

func (c *common) out(v any, human string) {
	if c.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(v)
		return
	}
	fmt.Println(human)
}

func parse(fs *flag.FlagSet, args []string) []string {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			os.Exit(2)
		}
		args = fs.Args()
		if len(args) == 0 {
			return pos
		}
		pos = append(pos, args[0])
		args = args[1:]
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `ethmix `+version+`: Ethereum / EVM JSON-RPC through the Nym mixnet

Mixnet client (official nym-socks5-client):
  ethmix nym setup [--binary PATH] [--port 1080]   init on a random active exit gateway, then start
  ethmix nym start | stop | status | exits

Local RPC gateway for any wallet or library (MetaMask, Rabby, Foundry, ethers, web3.py ...):
  ethmix serve [--listen 127.0.0.1:8545] [--default ethereum] [--add name=https://rpc,...]
     then add a custom network with RPC URL  http://127.0.0.1:8545/ethereum   (or /base, /arbitrum ...)

Talk to a public RPC directly over the mixnet:
  ethmix balance ADDRESS [--chain base]     native balance
  ethmix block [--chain ethereum]           latest block number and time
  ethmix call METHOD [JSON_PARAM ...]       any JSON-RPC method, e.g. call eth_getCode '"0x..."' '"latest"'
  ethmix send RAW_TX_HEX|FILE|-             broadcast a signed raw transaction (eth_sendRawTransaction)
  ethmix chains                             built-in networks and upstreams
  ethmix probe [--chain X]                  which upstreams work through the mixnet, and how fast
  ethmix ip                                 which IP do RPC providers see? (should be a Nym exit gateway)

Common flags: --chain NAME --rpc URL --proxy 127.0.0.1:1080 --dns doh|exit -v --json --timeout 5m
ethmix never holds a private key, and there is no clearnet fallback: if the
mixnet client is down, ethmix refuses to connect.
`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "nym":
		cmdNym(args)
	case "serve":
		cmdServe(args)
	case "call":
		cmdCall(args)
	case "balance":
		cmdBalance(args)
	case "block":
		cmdBlock(args)
	case "send":
		cmdSend(args)
	case "chains":
		cmdChains()
	case "probe":
		cmdProbe(args)
	case "ip":
		cmdIP(args)
	case "version", "-version", "--version":
		fmt.Println("ethmix", version)
	default:
		usage()
		os.Exit(2)
	}
}

func cmdChains() {
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "CHAIN\tID\tSYMBOL\tUPSTREAMS")
	for _, c := range rpc.Chains {
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\n", c.Name, c.ChainID, c.Symbol, strings.Join(c.URLs, " "))
	}
	tw.Flush()
}

func cmdServe(args []string) {
	var c common
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	c.register(fs)
	listen := fs.String("listen", "127.0.0.1:8545", "local address")
	def := fs.String("default", "ethereum", "chain served at /")
	add := fs.String("add", "", "extra networks: name=https://rpc[|https://rpc2],name2=...")
	hosts := fs.String("allow-host", "", "extra Host header values to accept (comma separated)")
	parse(fs, args)
	g := &gateway.Gateway{Client: c.client(), Default: *def, Log: logf, Custom: map[string][]string{}}
	g.Client.Log = nil
	if *hosts != "" {
		g.Hosts = strings.Split(*hosts, ",")
	}
	for _, kv := range strings.Split(*add, ",") {
		if kv == "" {
			continue
		}
		n, u, ok := strings.Cut(kv, "=")
		if !ok {
			die(fmt.Errorf("--add wants name=https://url, got %q", kv))
		}
		g.Custom[strings.ToLower(n)] = strings.Split(u, "|")
	}
	if c.verbose {
		g.Client.Log = tracef
	}
	if _, ok := rpc.ByName(*def); !ok {
		if _, ok := g.Custom[*def]; !ok {
			die(fmt.Errorf("unknown --default chain %q", *def))
		}
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		die(err)
	}
	if h, _, _ := net.SplitHostPort(*listen); h != "127.0.0.1" && h != "localhost" && h != "::1" {
		fmt.Fprintln(os.Stderr, "WARNING: gateway is not bound to loopback; anyone who can reach it can spend your mixnet bandwidth")
	}
	logf("ethmix gateway on http://%s  (default chain %s; SOCKS5 %s)", ln.Addr(), *def, c.proxy)
	srv := &http.Server{Handler: g}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); srv.Close() }()
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		die(err)
	}
}

func cmdCall(args []string) {
	var c common
	fs := flag.NewFlagSet("call", flag.ExitOnError)
	c.register(fs)
	pos := parse(fs, args)
	if len(pos) < 1 {
		die(fmt.Errorf("usage: ethmix call METHOD [JSON_PARAM ...]"))
	}
	var params []any
	for _, p := range pos[1:] {
		var v any
		if err := json.Unmarshal([]byte(p), &v); err != nil {
			die(fmt.Errorf("param %q is not JSON (quote strings: '\"0x..\"'): %v", p, err))
		}
		params = append(params, v)
	}
	ctx, cancel := c.ctx()
	defer cancel()
	res, used, err := c.client().Call(ctx, c.urls(), pos[0], params)
	if err != nil {
		die(err)
	}
	c.out(map[string]any{"upstream": used, "result": json.RawMessage(res)}, string(res))
}

var addrRe = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

func hexBig(raw json.RawMessage) (*big.Int, error) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return nil, err
	}
	n, ok := new(big.Int).SetString(strings.TrimPrefix(s, "0x"), 16)
	if !ok {
		return nil, fmt.Errorf("bad hex quantity %q", s)
	}
	return n, nil
}

func fmtUnits(n *big.Int, dec int) string {
	r := new(big.Rat).SetFrac(n, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(dec)), nil))
	s := r.FloatString(dec)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	if s == "" {
		s = "0"
	}
	return s
}

func cmdBalance(args []string) {
	var c common
	fs := flag.NewFlagSet("balance", flag.ExitOnError)
	c.register(fs)
	pos := parse(fs, args)
	if len(pos) != 1 || !addrRe.MatchString(pos[0]) {
		die(fmt.Errorf("usage: ethmix balance 0xADDRESS [--chain base]"))
	}
	ctx, cancel := c.ctx()
	defer cancel()
	res, used, err := c.client().Call(ctx, c.urls(), "eth_getBalance", []any{pos[0], "latest"})
	if err != nil {
		die(err)
	}
	n, err := hexBig(res)
	if err != nil {
		die(err)
	}
	sym := "ETH"
	if ch, ok := rpc.ByName(c.chain); ok && c.rpcURL == "" {
		sym = ch.Symbol
	}
	c.out(map[string]any{"address": pos[0], "wei": n.String(), "upstream": used},
		fmt.Sprintf("%s  %s %s  (%s wei, via %s over the mixnet)", pos[0], fmtUnits(n, 18), sym, n, hostOf(used)))
}

func hostOf(u string) string {
	u = strings.TrimPrefix(u, "https://")
	if i := strings.IndexByte(u, '/'); i >= 0 {
		u = u[:i]
	}
	return u
}

func cmdBlock(args []string) {
	var c common
	fs := flag.NewFlagSet("block", flag.ExitOnError)
	c.register(fs)
	parse(fs, args)
	ctx, cancel := c.ctx()
	defer cancel()
	cl := c.client()
	res, used, err := cl.Call(ctx, c.urls(), "eth_getBlockByNumber", []any{"latest", false})
	if err != nil {
		die(err)
	}
	var b struct{ Number, Timestamp, Hash string }
	if err := json.Unmarshal(res, &b); err != nil {
		die(err)
	}
	n, _ := new(big.Int).SetString(strings.TrimPrefix(b.Number, "0x"), 16)
	ts, _ := new(big.Int).SetString(strings.TrimPrefix(b.Timestamp, "0x"), 16)
	if n == nil || ts == nil {
		die(fmt.Errorf("unexpected block answer from %s", used))
	}
	age := time.Since(time.Unix(ts.Int64(), 0)).Round(time.Second)
	c.out(map[string]any{"number": n.String(), "hash": b.Hash, "timestamp": ts.Int64(), "upstream": used},
		fmt.Sprintf("block %s  %s  %s old (via %s over the mixnet)", n, b.Hash, age, hostOf(used)))
}

func cmdSend(args []string) {
	var c common
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	c.register(fs)
	pos := parse(fs, args)
	if len(pos) != 1 {
		die(fmt.Errorf("usage: ethmix send RAW_TX_HEX | FILE | -"))
	}
	raw := pos[0]
	if raw == "-" {
		b, _ := readAll(os.Stdin)
		raw = string(b)
	} else if !strings.HasPrefix(raw, "0x") && !isHex(raw) {
		b, err := os.ReadFile(raw)
		if err != nil {
			die(err)
		}
		raw = string(b)
	}
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "0x") {
		raw = "0x" + raw
	}
	if !isHex(raw[2:]) || len(raw) < 20 {
		die(fmt.Errorf("not a hex-encoded signed transaction"))
	}
	ctx, cancel := c.ctx()
	defer cancel()
	res, used, err := c.client().Call(ctx, c.urls(), "eth_sendRawTransaction", []any{raw})
	if err != nil {
		die(err)
	}
	var h string
	_ = json.Unmarshal(res, &h)
	c.out(map[string]any{"txhash": h, "upstream": used}, fmt.Sprintf("broadcast ok: %s (via %s over the mixnet)", h, hostOf(used)))
}

var hexRe = regexp.MustCompile(`^[0-9a-fA-F]+$`)

func isHex(s string) bool { return len(s)%2 == 0 && hexRe.MatchString(s) }

func cmdProbe(args []string) {
	var c common
	fs := flag.NewFlagSet("probe", flag.ExitOnError)
	c.register(fs)
	parse(fs, args)
	ctx, cancel := c.ctx()
	defer cancel()
	cl := c.client()
	var chains []rpc.Chain
	if c.rpcURL != "" {
		chains = []rpc.Chain{{Name: "custom", URLs: []string{c.rpcURL}}}
	} else if c.chain == "all" {
		chains = rpc.Chains
	} else {
		ch, ok := rpc.ByName(c.chain)
		if !ok {
			die(fmt.Errorf("unknown chain %q", c.chain))
		}
		chains = []rpc.Chain{ch}
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "CHAIN\tUPSTREAM\tRESULT\tTIME")
	for _, ch := range chains {
		for _, u := range ch.URLs {
			t0 := time.Now()
			res, _, err := cl.Call(ctx, []string{u}, "eth_blockNumber", nil)
			el := time.Since(t0).Seconds()
			if err != nil {
				e := err.Error()
				if len(e) > 70 {
					e = e[:70]
				}
				fmt.Fprintf(tw, "%s\t%s\tFAIL %s\t%.1fs\n", ch.Name, hostOf(u), e, el)
			} else if n, err := hexBig(res); err == nil {
				fmt.Fprintf(tw, "%s\t%s\tblock %s\t%.1fs\n", ch.Name, hostOf(u), n, el)
			} else {
				fmt.Fprintf(tw, "%s\t%s\tFAIL bad answer\t%.1fs\n", ch.Name, hostOf(u), el)
			}
			tw.Flush()
		}
	}
}

func cmdIP(args []string) {
	var c common
	fs := flag.NewFlagSet("ip", flag.ExitOnError)
	c.register(fs)
	noLookup := fs.Bool("no-lookup", false, "don't look the IP up in the nym-api node list")
	parse(fs, args)
	d := c.dialer()
	ctx, cancel := c.ctx()
	defer cancel()
	showIP(ctx, &c, d, *noLookup)
}

func readAll(f *os.File) ([]byte, error) {
	var b []byte
	buf := make([]byte, 4096)
	for {
		n, err := f.Read(buf)
		b = append(b, buf[:n]...)
		if err != nil {
			return b, nil
		}
	}
}
