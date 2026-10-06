package main

import (
	"strings"
	"testing"
)

func TestSSERequiresCompleteVisibleAnswer(t *testing.T) {
	cases := []struct {
		name, body string
		valid      bool
		tokens     int
	}{
		{"complete", "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n", true, 1},
		{"role only", "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\ndata: [DONE]\n\n", false, 0},
		{"missing DONE", "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n", false, 1},
		{"unfinished event", "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n", false, 1},
		{"multiline", "data: {\"choices\":\ndata: [{\"delta\":{\"content\":\"hello\"}}]}\n\ndata: [DONE]\n\n", true, 1},
		{"bad JSON", "data: garbage\n\n", false, 0},
		{"after DONE", "data: [DONE]\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n", false, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tokens := 0
			e := consumeSSE(strings.NewReader(c.body), func() { tokens++ })
			if (e == nil) != c.valid || tokens != c.tokens {
				t.Fatalf("err=%v tokens=%d", e, tokens)
			}
		})
	}
}

func TestDockerMemoryUnits(t *testing.T) {
	for _, c := range []struct {
		value string
		want  float64
	}{{"2MiB / 256MiB", 2}, {"1GiB / 2GiB", 1024}, {"512KiB / 1GiB", .5}, {"1048576B / 2GiB", 1}} {
		got, e := memoryMiB(c.value)
		if e != nil || got != c.want {
			t.Fatalf("%s: %v %v", c.value, got, e)
		}
	}
	if _, e := memoryMiB("unknown"); e == nil {
		t.Fatal("invalid unit accepted")
	}
}
