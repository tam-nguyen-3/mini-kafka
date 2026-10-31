package transport

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/tam-nguyen-3/mini-kafka/internal/controller"
)

// NormalizeAddress converts a destination to a canonical HTTP(S) base URL.
// Bind/listen addresses are separate: wildcard addresses cannot be advertised.
func NormalizeAddress(addr string) (string, error) {
	invalid := func() (string, error) { return "", fmt.Errorf("%w: %q", controller.ErrInvalidBrokerAddress, addr) }
	if addr == "" || strings.TrimSpace(addr) != addr {
		return invalid()
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return invalid()
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || host == "" || host == "*" || strings.ContainsAny(host, " /\\%?#") {
		return invalid()
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return invalid()
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return invalid()
	}
	return u.Scheme + "://" + net.JoinHostPort(strings.ToLower(host), strconv.Itoa(n)), nil
}
