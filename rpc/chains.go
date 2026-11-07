// Package rpc talks Ethereum JSON-RPC to public endpoints through the Nym mixnet.
package rpc

import "strings"

// Chain is a network with a list of public, keyless RPC endpoints (tried in order).
type Chain struct {
	Name    string
	ChainID int
	Symbol  string
	URLs    []string
}

// Chains are the built-in networks. Any other endpoint can be given with --rpc.
var Chains = []Chain{
	{"ethereum", 1, "ETH", []string{"https://ethereum-rpc.publicnode.com", "https://eth.llamarpc.com", "https://cloudflare-eth.com"}},
	{"base", 8453, "ETH", []string{"https://base-rpc.publicnode.com", "https://mainnet.base.org"}},
	{"arbitrum", 42161, "ETH", []string{"https://arbitrum-one-rpc.publicnode.com", "https://arb1.arbitrum.io/rpc"}},
	{"optimism", 10, "ETH", []string{"https://optimism-rpc.publicnode.com", "https://mainnet.optimism.io"}},
	{"polygon", 137, "POL", []string{"https://polygon-bor-rpc.publicnode.com", "https://polygon-rpc.com"}},
	{"bnb", 56, "BNB", []string{"https://bsc-rpc.publicnode.com", "https://bsc-dataseed.bnbchain.org"}},
	{"gnosis", 100, "xDAI", []string{"https://gnosis-rpc.publicnode.com", "https://rpc.gnosischain.com"}},
	{"sepolia", 11155111, "ETH", []string{"https://ethereum-sepolia-rpc.publicnode.com", "https://rpc.sepolia.org"}},
}

// ByName finds a chain by name (case-insensitive) or chain id.
func ByName(s string) (Chain, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	for _, c := range Chains {
		if c.Name == s {
			return c, true
		}
	}
	return Chain{}, false
}
