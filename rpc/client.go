package rpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apeirs/ethmix/mixnet"
)

// Client posts JSON-RPC bodies to upstream endpoints, always via the mixnet Dialer.
// There is no clearnet fallback.
type Client struct {
	D       *mixnet.Dialer
	Timeout time.Duration // per upstream attempt
	Log     func(format string, args ...any)

	http  *http.Client
	Calls atomic.Int64
}

func (c *Client) client() *http.Client {
	if c.http == nil {
		// Keep-alives on: one mixnet stream + TLS handshake is expensive, reuse it.
		c.http = &http.Client{Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         c.D.DialContext,
			TLSHandshakeTimeout: 90 * time.Second,
			TLSClientConfig:     &tls.Config{RootCAs: c.D.RootCAs},
			IdleConnTimeout:     90 * time.Second,
			MaxIdleConnsPerHost: 4,
		}}
	}
	return c.http
}

func (c *Client) logf(f string, a ...any) {
	if c.Log != nil {
		c.Log(f, a...)
	}
}

// Raw sends body (a single request or a batch) to the first upstream that
// returns a well-formed JSON-RPC answer. It returns the answer and the upstream used.
func (c *Client) Raw(ctx context.Context, urls []string, body []byte) ([]byte, string, error) {
	if len(urls) == 0 {
		return nil, "", errors.New("no upstream RPC URL")
	}
	var errs []error
	var fallback []byte
	for _, u := range urls {
		pu, err := url.Parse(u)
		if err != nil || pu.Scheme != "https" {
			errs = append(errs, fmt.Errorf("%s: only https upstreams are allowed", u))
			continue
		}
		tmo := c.Timeout
		if tmo == 0 {
			tmo = 50 * time.Second
		}
		actx, cancel := context.WithTimeout(ctx, tmo)
		t0 := time.Now()
		out, err := c.once(actx, u, body)
		cancel()
		if err != nil {
			c.logf("rpc  %s failed after %.2fs: %v", pu.Host, time.Since(t0).Seconds(), err)
			errs = append(errs, fmt.Errorf("%s: %w", pu.Host, err))
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if code, bad := serverError(out); bad {
			// Provider-side hiccup (internal error, rate limit): try the next upstream.
			c.logf("rpc  %s answered with server error %d, trying the next upstream", pu.Host, code)
			errs = append(errs, fmt.Errorf("%s: server error %d", pu.Host, code))
			fallback = out
			continue
		}
		c.Calls.Add(1)
		c.logf("rpc  %s answered in %.2fs (%d bytes)", pu.Host, time.Since(t0).Seconds(), len(out))
		return out, u, nil
	}
	if fallback != nil {
		return fallback, "", nil
	}
	return nil, "", errors.Join(errs...)
}

// serverError reports a single (non-batch) answer that carries a provider-side error.
func serverError(out []byte) (int, bool) {
	var r Response
	if json.Unmarshal(out, &r) != nil || r.Error == nil {
		return 0, false
	}
	switch r.Error.Code {
	case -32603, -32005, -32097, -32001:
		return r.Error.Code, true
	}
	return 0, false
}

func (c *Client) once(ctx context.Context, u string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	// Nothing about the local caller is forwarded: no Origin, no browser UA.
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := c.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d: %.120s", resp.StatusCode, strings.TrimSpace(string(out)))
	}
	if !json.Valid(out) {
		return nil, fmt.Errorf("not JSON: %.80q", out)
	}
	return out, nil
}

// Response is a decoded JSON-RPC answer.
type Response struct {
	Result json.RawMessage `json:"result"`
	Error  *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// Call performs one JSON-RPC method call.
func (c *Client) Call(ctx context.Context, urls []string, method string, params []any) (json.RawMessage, string, error) {
	if params == nil {
		params = []any{}
	}
	body, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	out, used, err := c.Raw(ctx, urls, body)
	if err != nil {
		return nil, used, err
	}
	var r Response
	if err := json.Unmarshal(out, &r); err != nil {
		return nil, used, err
	}
	if r.Error != nil {
		return nil, used, fmt.Errorf("RPC error %d: %s", r.Error.Code, r.Error.Message)
	}
	return r.Result, used, nil
}
