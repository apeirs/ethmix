// Package gateway is a local JSON-RPC endpoint for wallets and libraries:
// point MetaMask, Rabby, Foundry, ethers or web3.py at http://127.0.0.1:8545/<chain>
// and every call is forwarded to a public RPC through the Nym mixnet.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apeirs/ethmix/rpc"
)

// Gateway serves http://listen/<chain>.
type Gateway struct {
	Client  *rpc.Client
	Default string              // chain served at "/"
	Custom  map[string][]string // extra chain name -> upstream URLs (overrides built-ins)
	Hosts   []string            // extra accepted Host values
	Timeout time.Duration
	Log     func(string, ...any)

	Total, Failed atomic.Int64
}

func (g *Gateway) upstreams(name string) ([]string, bool) {
	name = strings.ToLower(name)
	if u, ok := g.Custom[name]; ok {
		return u, true
	}
	if c, ok := rpc.ByName(name); ok {
		return c.URLs, true
	}
	return nil, false
}

func (g *Gateway) hostOK(h string) bool {
	if hh, _, err := net.SplitHostPort(h); err == nil {
		h = hh
	}
	h = strings.Trim(h, "[]")
	switch h {
	case "127.0.0.1", "localhost", "::1":
		return true
	}
	for _, x := range g.Hosts {
		if x == h {
			return true
		}
	}
	return false
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// DNS-rebinding guard: a web page must not reach this through a rebound name.
	if !g.hostOK(r.Host) {
		http.Error(w, "bad Host header", http.StatusForbidden)
		return
	}
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "content-type")
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return
	}
	chain := strings.Trim(r.URL.Path, "/")
	if chain == "" {
		chain = g.Default
	}
	urls, ok := g.upstreams(chain)
	if r.Method == "GET" || !ok {
		g.help(w, ok || chain == "")
		return
	}
	if r.Method != "POST" {
		http.Error(w, "POST JSON-RPC", 405)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 8<<20))
	if err != nil || !json.Valid(body) {
		http.Error(w, "invalid JSON", 400)
		return
	}
	g.Total.Add(1)
	tmo := g.Timeout
	if tmo == 0 {
		tmo = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(r.Context(), tmo)
	defer cancel()
	t0 := time.Now()
	out, used, err := g.Client.Raw(ctx, urls, body)
	if err != nil {
		g.Failed.Add(1)
		g.logf("%s %s FAILED after %.1fs: %v", chain, methods(body), time.Since(t0).Seconds(), err)
		h.Set("Content-Type", "application/json")
		w.WriteHeader(502)
		msg, _ := json.Marshal(fmt.Sprintf("ethmix: all upstreams failed over the mixnet: %v", err))
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":%s}}`, msg)
		return
	}
	g.logf("%s %s -> mixnet -> %s in %.1fs", chain, methods(body), hostOf(used), time.Since(t0).Seconds())
	h.Set("Content-Type", "application/json")
	w.Write(out)
}

func hostOf(u string) string {
	u = strings.TrimPrefix(u, "https://")
	if i := strings.IndexByte(u, '/'); i >= 0 {
		u = u[:i]
	}
	return u
}

// methods names the RPC methods in a request, for the log (params are never logged).
func methods(body []byte) string {
	var one struct{ Method string }
	if json.Unmarshal(body, &one) == nil && one.Method != "" {
		return one.Method
	}
	var batch []struct{ Method string }
	if json.Unmarshal(body, &batch) == nil {
		var ms []string
		for _, b := range batch {
			ms = append(ms, b.Method)
		}
		return "[" + strings.Join(ms, ",") + "]"
	}
	return "?"
}

func (g *Gateway) logf(f string, a ...any) {
	if g.Log != nil {
		g.Log(f, a...)
	}
}

func (g *Gateway) help(w http.ResponseWriter, okChain bool) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintf(w, "ethmix gateway: JSON-RPC over the Nym mixnet.\nPOST to /<chain>:\n")
	for _, c := range rpc.Chains {
		fmt.Fprintf(w, "  /%-9s chain id %-8d (%d upstreams)\n", c.Name, c.ChainID, len(c.URLs))
	}
	for n, u := range g.Custom {
		fmt.Fprintf(w, "  /%-9s custom (%d upstreams)\n", n, len(u))
	}
	fmt.Fprintf(w, "requests so far: %d, failed: %d\n", g.Total.Load(), g.Failed.Load())
}
