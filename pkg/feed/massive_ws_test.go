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

func TestParseMassiveEvents_TradesAndStrings(t *testing.T) {
	// 1. Verbatim wire frame with string ds: "40.0"
	rawStringDS := `[{"ev":"T","sym":"AAPL","i":"143541","x":4,"p":297.845,"s":40,"t":1781622376036,"pt":1781622376029,"q":4938312,"z":3,"trfi":202,"trft":1781622376036,"ds":"40.0"}]`
	ticks, err := ParseMassiveEvents([]byte(rawStringDS))
	if err != nil {
		t.Fatalf("Failed to parse string ds: %v", err)
	}
	if len(ticks) != 1 {
		t.Fatalf("expected 1 tick, got %d", len(ticks))
	}
	if ticks[0].Type != TickTrade || ticks[0].Price != 297.845 || ticks[0].Size != 40.0 || ticks[0].Symbol != "AAPL" {
		t.Errorf("mismatch on string ds tick: %+v", ticks[0])
	}

	// 2. Fractional shares string ds: "0.740474"
	rawFractional := `[{"ev":"T","sym":"QQQ","p":744.15,"s":1,"ds":"0.740474","t":1790341395094}]`
	ticks, err = ParseMassiveEvents([]byte(rawFractional))
	if err != nil {
		t.Fatalf("Failed to parse fractional ds: %v", err)
	}
	if len(ticks) != 1 || ticks[0].Size != 0.740474 {
		t.Errorf("mismatch on fractional ds tick: %+v", ticks)
	}

	// 3. Fallback to s when ds is omitted or 0
	rawOmittedDS := `[{"ev":"T","sym":"SPY","p":765.5,"s":120,"t":1790341395000}]`
	ticks, err = ParseMassiveEvents([]byte(rawOmittedDS))
	if err != nil {
		t.Fatalf("Failed to parse omitted ds: %v", err)
	}
	if len(ticks) != 1 || ticks[0].Size != 120.0 {
		t.Errorf("mismatch on omitted ds tick: %+v", ticks)
	}

	// 4. Single JSON object {...}
	rawSingle := `{"ev":"T","sym":"NVDA","p":120.0,"s":50,"ds":"50.0","t":1790341395100}`
	ticks, err = ParseMassiveEvents([]byte(rawSingle))
	if err != nil {
		t.Fatalf("Failed to parse single json object: %v", err)
	}
	if len(ticks) != 1 || ticks[0].Symbol != "NVDA" || ticks[0].Size != 50.0 {
		t.Errorf("mismatch on single json object: %+v", ticks)
	}

	// 5. Status event (should not emit market ticks)
	rawStatus := `[{"ev":"status","status":"success","message":"subscribed to: Q.AAPL, T.AAPL"}]`
	ticks, err = ParseMassiveEvents([]byte(rawStatus))
	if err != nil {
		t.Fatalf("Failed to parse status event: %v", err)
	}
	if len(ticks) != 0 {
		t.Errorf("expected 0 ticks for status event, got %d", len(ticks))
	}

	// 6. Invalid trades with price <= 0, size <= 0, or timestamp <= 0 should be dropped
	rawInvalid := `[
		{"ev":"T","sym":"BAD1","p":0.0,"s":10,"ds":"10.0","t":1790341395000},
		{"ev":"T","sym":"BAD2","p":100.0,"s":0,"ds":"0.0","t":1790341395000},
		{"ev":"T","sym":"BAD3","p":100.0,"s":10,"ds":"10.0","t":0}
	]`
	ticks, err = ParseMassiveEvents([]byte(rawInvalid))
	if err != nil {
		t.Fatalf("Failed to parse rawInvalid: %v", err)
	}
	if len(ticks) != 0 {
		t.Errorf("expected 0 ticks for invalid trades, got %d", len(ticks))
	}

	// 7. Non-numeric invalid string in ds should return error
	rawGarbage := `[{"ev":"T","sym":"AAPL","p":100.0,"s":10,"ds":"NOT_A_NUMBER","t":1790341395000}]`
	_, err = ParseMassiveEvents([]byte(rawGarbage))
	if err == nil {
		t.Errorf("expected error on non-numeric ds string, got nil")
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
