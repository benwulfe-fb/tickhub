package feed

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestFormatSubscription(t *testing.T) {
	symbols := []string{"DASH", "CRM", "DELL"}
	sub := FormatSubscription(symbols)
	expected := "Q.DASH,T.DASH,Q.CRM,T.CRM,Q.DELL,T.DELL"
	if sub != expected {
		t.Fatalf("FormatSubscription expected %s, got %s", expected, sub)
	}
}

func TestParseMassiveEvents(t *testing.T) {
	rawJSON := `[
		{"ev":"Q","sym":"DASH","bp":170.5,"ap":170.6,"bs":5,"as":3,"t":1778074266044},
		{"ev":"T","sym":"DASH","p":170.55,"s":100,"ds":100.5,"t":1778074266050}
	]`

	ticks, err := ParseMassiveEvents([]byte(rawJSON))
	if err != nil {
		t.Fatalf("ParseMassiveEvents failed: %v", err)
	}

	if len(ticks) != 2 {
		t.Fatalf("expected 2 ticks, got %d", len(ticks))
	}

	// Verify Quote
	q := ticks[0]
	if q.Type != TickQuote {
		t.Errorf("expected TickQuote, got %v", q.Type)
	}
	if q.Symbol != "DASH" {
		t.Errorf("expected DASH, got %s", q.Symbol)
	}
	if q.SIPTimestampNS != 1778074266044*1_000_000 {
		t.Errorf("timestamp mismatch: %d", q.SIPTimestampNS)
	}
	if q.BidPx != 170.5 || q.AskPx != 170.6 || q.BidSz != 5 || q.AskSz != 3 {
		t.Errorf("quote field mismatch: %+v", q)
	}

	// Verify Trade
	tr := ticks[1]
	if tr.Type != TickTrade {
		t.Errorf("expected TickTrade, got %v", tr.Type)
	}
	if tr.Price != 170.55 || tr.Size != 100.5 {
		t.Errorf("trade price/size mismatch: %+v", tr)
	}
}

func TestMassiveWSClientMockServer(t *testing.T) {
	upgrader := websocket.Upgrader{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade failed: %v", err)
			return
		}
		defer c.Close()

		// 1. Read Auth
		_, authMsg, err := c.ReadMessage()
		if err != nil {
			return
		}
		var authData map[string]string
		json.Unmarshal(authMsg, &authData)
		if authData["action"] != "auth" || authData["params"] != "TEST_API_KEY" {
			t.Errorf("unexpected auth: %s", string(authMsg))
			return
		}

		// Reply auth success
		c.WriteJSON([]RawMassiveEvent{
			{Ev: "status", Status: "auth_success", Msg: "authenticated"},
		})

		// 2. Read Subscribe
		_, subMsg, err := c.ReadMessage()
		if err != nil {
			return
		}
		var subData map[string]string
		json.Unmarshal(subMsg, &subData)
		if subData["action"] != "subscribe" || subData["params"] != "Q.DASH,T.DASH" {
			t.Errorf("unexpected subscribe: %s", string(subMsg))
			return
		}

		// 3. Emit test tick stream
		c.WriteJSON([]RawMassiveEvent{
			{Ev: "Q", Sym: "DASH", Bp: 170.1, Ap: 170.2, Bs: 10, As: 12, T: 1778074260000},
			{Ev: "T", Sym: "DASH", P: 170.15, S: 50, T: 1778074260050},
		})

		// Idle wait for client disconnect
		time.Sleep(100 * time.Millisecond)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	client := NewMassiveWSClient(wsURL, "TEST_API_KEY", []string{"DASH"}, 1024)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("client.Start failed: %v", err)
	}
	defer client.Close()

	// Drain 2 ticks
	ticksChan := client.Ticks()

	select {
	case t1 := <-ticksChan:
		if t1.Type != TickQuote || t1.BidPx != 170.1 {
			t.Errorf("unexpected tick 1: %+v", t1)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tick 1")
	}

	select {
	case t2 := <-ticksChan:
		if t2.Type != TickTrade || t2.Price != 170.15 {
			t.Errorf("unexpected tick 2: %+v", t2)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for tick 2")
	}
}
