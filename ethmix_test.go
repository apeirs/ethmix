package ethmix_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/apeirs/ethmix/gateway"
	"github.com/apeirs/ethmix/internal/fakes"
	"github.com/apeirs/ethmix/mixnet"
	"github.com/apeirs/ethmix/rpc"
)

type world struct {
	socks *fakes.SOCKS
	cl    *rpc.Client
	got   chan string // JSON bodies seen by the fake RPC
	hdrs  chan http.Header
}

func newWorld(t *testing.T) *world {
	t.Helper()
	ca := fakes.NewCA()
	w := &world{got: make(chan string, 16), hdrs: make(chan http.Header, 16)}
	serve := func(h http.HandlerFunc, name string) string {
		ln, _ := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{ca.Leaf(name)}})
		srv := &http.Server{Handler: h}
		go srv.Serve(ln)
		t.Cleanup(func() { srv.Close() })
		return ln.Addr().String()
	}
	good := serve(func(rw http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		w.got <- string(b)
		w.hdrs <- r.Header.Clone()
		var q struct {
			ID     json.RawMessage
			Method string
		}
		json.Unmarshal(b, &q)
		rw.Header().Set("Content-Type", "application/json")
		if q.Method == "eth_busy" {
			io.WriteString(rw, `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"Internal error"}}`)
			return
		}
		io.WriteString(rw, `{"jsonrpc":"2.0","id":`+string(q.ID)+`,"result":"0x1234"}`)
	}, "rpc.test")
	bad := serve(func(rw http.ResponseWriter, r *http.Request) { http.Error(rw, "boom", 500) }, "bad.test")
	doh := fakes.DoH(ca, "1.1.1.1", map[string][]string{"rpc.test": {"10.0.0.9"}, "bad.test": {"10.0.0.8"}})
	w.socks = fakes.NewSOCKS(map[string]string{"1.1.1.1:443": doh, "10.0.0.9:443": good, "10.0.0.8:443": bad})
	t.Cleanup(w.socks.Close)
	d := &mixnet.Dialer{Proxy: w.socks.Addr, DNS: mixnet.DNSDoH, RootCAs: ca.Pool, Resolvers: []string{"1.1.1.1"}, Timeout: 5 * time.Second}
	w.cl = &rpc.Client{D: d, Timeout: 10 * time.Second}
	return w
}

func post(t *testing.T, url, host, body string) (*http.Response, string) {
	req, _ := http.NewRequest("POST", url, strings.NewReader(body))
	if host != "" {
		req.Host = host
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

// An unmodified client talks to http://gateway/<chain>; the RPC gets the
// body, and every connection went through the SOCKS proxy (DoH, then IPv4).
func TestGatewayForwardsOnlyThroughMixnet(t *testing.T) {
	w := newWorld(t)
	g := &gateway.Gateway{Client: w.cl, Default: "net", Custom: map[string][]string{"net": {"https://bad.test", "https://rpc.test"}}}
	srv := httptest.NewServer(g)
	defer srv.Close()
	resp, body := post(t, srv.URL+"/net", "", `{"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}`)
	if resp.StatusCode != 200 || !strings.Contains(body, `"id":7`) || !strings.Contains(body, "0x1234") {
		t.Fatalf("got %d %s", resp.StatusCode, body)
	}
	if got := <-w.got; !strings.Contains(got, "eth_blockNumber") {
		t.Fatalf("upstream saw %q", got)
	}
	h := <-w.hdrs
	if h.Get("Origin") != "" || h.Get("Referer") != "" || h.Get("User-Agent") != "Mozilla/5.0" {
		t.Fatalf("local caller leaked into upstream headers: %v", h)
	}
	seen := map[string]bool{}
	for _, r := range w.socks.Seen() {
		seen[r.Host] = true
		if r.ATYP == 3 {
			t.Errorf("hostname %q sent to the exit; DoH mode must connect by IPv4", r.Host)
		}
	}
	for _, want := range []string{"1.1.1.1", "10.0.0.8", "10.0.0.9"} {
		if !seen[want] {
			t.Errorf("SOCKS never saw %s (saw %v)", want, seen)
		}
	}
	// Default chain at "/", batches pass through untouched.
	_, body = post(t, srv.URL+"/", "", `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}]`)
	if !strings.Contains(body, "0x1234") {
		t.Fatalf("default chain: %s", body)
	}
}

func TestFailoverOnServerError(t *testing.T) {
	w := newWorld(t)
	_, out, err := w.cl.Call(context.Background(), []string{"https://rpc.test"}, "eth_busy", nil)
	_ = out
	// Only upstream answers a server error: surfaced, not hidden.
	if err == nil || !strings.Contains(err.Error(), "-32603") {
		t.Fatalf("want the server error, got %v", err)
	}
	res, used, err := w.cl.Call(context.Background(), []string{"https://bad.test", "https://rpc.test"}, "eth_chainId", nil)
	if err != nil || string(res) != `"0x1234"` || used != "https://rpc.test" {
		t.Fatalf("failover: %s %s %v", res, used, err)
	}
}

func TestRebindingAndNoClearnetFallback(t *testing.T) {
	w := newWorld(t)
	g := &gateway.Gateway{Client: w.cl, Default: "net", Custom: map[string][]string{"net": {"https://rpc.test"}}}
	srv := httptest.NewServer(g)
	defer srv.Close()
	resp, _ := post(t, srv.URL, "evil.example", `{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}`)
	if resp.StatusCode != 403 {
		t.Fatalf("rebinding Host accepted: %d", resp.StatusCode)
	}
	// Mixnet client down: refuse, never connect directly.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()
	dead2 := &rpc.Client{D: &mixnet.Dialer{Proxy: dead, Timeout: time.Second}, Timeout: 3 * time.Second}
	g2 := httptest.NewServer(&gateway.Gateway{Client: dead2, Default: "net", Custom: map[string][]string{"net": {"https://rpc.test"}}})
	defer g2.Close()
	resp, body := post(t, g2.URL, "", `{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}`)
	if resp.StatusCode != 502 || !strings.Contains(body, "mixnet") {
		t.Fatalf("want 502 mixnet error, got %d %s", resp.StatusCode, body)
	}
}

func TestOnlyHTTPSUpstreams(t *testing.T) {
	w := newWorld(t)
	if _, _, err := w.cl.Call(context.Background(), []string{"http://rpc.test"}, "eth_chainId", nil); err == nil {
		t.Fatal("plain http upstream accepted")
	}
}

func TestBuiltinChains(t *testing.T) {
	for _, n := range []string{"ethereum", "base", "arbitrum", "optimism", "polygon", "bnb", "gnosis", "sepolia"} {
		c, ok := rpc.ByName(n)
		if !ok || len(c.URLs) == 0 || c.ChainID == 0 {
			t.Errorf("chain %s", n)
		}
	}
}
