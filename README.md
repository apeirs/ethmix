# ethmix

Ethereum and EVM JSON-RPC through the [Nym mixnet](https://nym.com). One Go binary, MIT.

```
ethmix nym setup                 # starts nym-socks5-client on a random active exit gateway
ethmix serve                     # local RPC gateway: http://127.0.0.1:8545/<chain> -> mixnet -> public RPC
ethmix balance 0xADDR --chain base
ethmix block / call / send       # talk to a public RPC from the command line, over the mixnet
ethmix probe --chain all         # which public RPCs answer Nym exit traffic, and how fast
ethmix ip                        # the IP providers see (a Nym exit gateway)
```

## Why

Every wallet request to an RPC provider (`eth_getBalance`, `eth_call`, `eth_sendRawTransaction`) tells the provider
"this IP is interested in this address", and a broadcast says "this IP sent this transaction". RPC providers log
that, and with a handful of addresses it identifies you. Wallets let you set a custom RPC URL but not a proxy, so
`ethmix serve` is a plain `http://127.0.0.1:8545/<chain>` URL that works anywhere a custom RPC is accepted
(MetaMask, Rabby, Frame, Foundry, Hardhat, ethers, viem, web3.py): the provider then sees a Nym exit gateway, not you.

## Use

```
export ETHMIX_NYM_SOCKS5=/path/to/nym-socks5-client    # official release binary
go install github.com/apeirs/ethmix/cmd/ethmix@latest
ethmix nym setup
ethmix serve &
cast balance vitalik.eth --rpc-url http://127.0.0.1:8545/ethereum
```

Chains: `ethereum base arbitrum optimism polygon bnb gnosis sepolia` (several keyless public RPCs each, tried in
order; a timeout, HTTP error or provider-side `-32603`/rate-limit answer moves to the next one). Own endpoints:
`ethmix serve --add mychain=https://rpc1|https://rpc2`, or `--rpc URL` on the CLI commands. Upstreams must be https.

## How it uses the mixnet

- Drives the official `nym-socks5-client`; ethmix does no mixing itself. `nym setup` picks a random active exit
  gateway from the Nym API (exit-policy network requester), with reply SURBs on so the exit does not learn your
  mixnet address.
- Each upstream connection is a SOCKS5 stream through entry gateway, three mix layers and the exit. TLS runs end to
  end with the RPC provider; keep-alive reuses a stream so a wallet's polling does not pay a new handshake each time.
- Hostnames are resolved by DNS-over-HTTPS *through the mixnet*, then connected by IPv4. The local resolver never
  sees which provider you use, and a broken IPv6 route at the exit cannot stall the request.
- No clearnet fallback: with the mixnet client down, the gateway answers 502 and the CLI refuses.
- Nothing from the local caller is forwarded (no `Origin`, cookies, wallet User-Agent). The gateway listens on
  loopback only and rejects foreign `Host` headers (DNS-rebinding guard). Request params are never logged.
- ethmix never holds a key. `send` only forwards an already signed transaction.

## Limits (read these)

- It hides your IP, not your address: the provider still sees which addresses you query. Use one fresh-looking
  address per purpose; ethmix removes the IP link, not the on-chain one.
- All requests share one exit IP per client, so a provider can link your requests to each other. Mixnet latency is
  seconds (10 to 20 s per call in tests), fine for balances and broadcasts, slow for dapps.
- Public RPCs may block Nym exits or rate-limit them; `ethmix probe` shows what works now.
- Websocket subscriptions (`eth_subscribe`) are not supported, HTTP JSON-RPC only.
- Does not cover the dapp's own web traffic or token-list/price fetches your wallet makes itself.

## Tests

`go test ./...` runs against a fake mixnet (recording SOCKS5 proxy, DoH server, TLS RPC): everything goes through the
proxy and nothing is resolved locally, failover, header stripping, the rebinding rule, no fallback, https-only.
Live logs against the real mainnet mixnet are in `examples/`.
