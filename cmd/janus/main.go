// Janus is a streaming LLM gateway with tenant quotas and durable usage history.
package main

import "janus-proxy/internal/gateway"

func main() {
	gateway.Run()
}
