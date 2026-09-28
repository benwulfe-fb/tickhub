package feed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	DefaultMassiveWSURL = "wss://socket.massive.com/stocks"
	NSPerMS             = 1_000_000
)

// FlexFloat64 unmarshals JSON numbers, quoted strings (e.g. "40.0"), or nulls into float64.
type FlexFloat64 float64

func (f *FlexFloat64) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*f = 0
		return nil
	}
	if trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		trimmed = trimmed[1 : len(trimmed)-1]
	}
	if len(trimmed) == 0 {
		*f = 0
		return nil
	}
	val, err := strconv.ParseFloat(string(trimmed), 64)
	if err != nil {
		return fmt.Errorf("invalid FlexFloat64 %q: %w", string(data), err)
	}
	*f = FlexFloat64(val)
	return nil
}

func (f FlexFloat64) Float64() float64 {
	return float64(f)
}

// RawMassiveEvent matches the Polygon/Massive.com JSON event schema.
type RawMassiveEvent struct {
	Ev     string  `json:"ev"`
	Sym    string  `json:"sym"`
	Status string  `json:"status,omitempty"`
	Msg    string  `json:"message,omitempty"`
	// Quote fields
	Bp float64 `json:"bp,omitempty"`
	Ap float64 `json:"ap,omitempty"`
	Bs float64 `json:"bs,omitempty"`
	As float64 `json:"as,omitempty"`
	// Trade fields
	P  float64     `json:"p,omitempty"`
	S  float64     `json:"s,omitempty"`
	Ds FlexFloat64 `json:"ds,omitempty"`
	// Timestamp in SIP milliseconds
	T int64 `json:"t,omitempty"`
}

// FormatSubscription formats explicit Q.<sym>,T.<sym> subscription string.
func FormatSubscription(symbols []string) string {
	parts := make([]string, 0, len(symbols)*2)
	for _, s := range symbols {
		clean := strings.TrimSpace(s)
		if clean != "" {
			parts = append(parts, "Q."+clean, "T."+clean)
		}
	}
	return strings.Join(parts, ",")
}

// ParseMassiveEvents parses a JSON array or single Massive.com event into Tick slice.
func ParseMassiveEvents(data []byte) ([]Tick, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, nil
	}

	var events []RawMassiveEvent
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &events); err != nil {
			return nil, fmt.Errorf("unmarshal massive events array: %w", err)
		}
	} else if trimmed[0] == '{' {
		var single RawMassiveEvent
		if err := json.Unmarshal(trimmed, &single); err != nil {
			return nil, fmt.Errorf("unmarshal massive single event: %w", err)
		}
		events = []RawMassiveEvent{single}
	} else {
		maxLen := len(trimmed)
		if maxLen > 50 {
			maxLen = 50
		}
		return nil, fmt.Errorf("unexpected json format (does not start with [ or {): %s", string(trimmed[:maxLen]))
	}

	ticks := make([]Tick, 0, len(events))
	for _, ev := range events {
		switch ev.Ev {
		case "Q":
			ticks = append(ticks, Tick{
				SIPTimestampNS: ev.T * NSPerMS,
				Symbol:         ev.Sym,
				Type:           TickQuote,
				BidPx:          ev.Bp,
				AskPx:          ev.Ap,
				BidSz:          ev.Bs,
				AskSz:          ev.As,
			})
		case "T":
			sz := ev.Ds.Float64()
			if sz <= 0 {
				sz = ev.S
			}
			if ev.P <= 0 || sz <= 0 || ev.T <= 0 {
				continue
			}
			ticks = append(ticks, Tick{
				SIPTimestampNS: ev.T * NSPerMS,
				Symbol:         ev.Sym,
				Type:           TickTrade,
				Price:          ev.P,
				Size:           sz,
			})
		case "status":
			log.Printf("[MASSIVE-WS] Status event: status=%s message=%s", ev.Status, ev.Msg)
		}
	}
	return ticks, nil
}

// MassiveWSClient connects to Massive.com WebSocket, authenticates, and streams normalized Ticks.
type MassiveWSClient struct {
	url       string
	apiKey    string
	symbols   []string
	tickChan  chan Tick
	stopChan  chan struct{}
	closeOnce sync.Once
	chanOnce  sync.Once
	conn      *websocket.Conn
	mu        sync.Mutex
	wg        sync.WaitGroup
}

// NewMassiveWSClient constructs a new client instance.
func NewMassiveWSClient(url, apiKey string, symbols []string, bufferSize int) *MassiveWSClient {
	if url == "" {
		url = DefaultMassiveWSURL
	}
	if bufferSize <= 0 {
		bufferSize = 65536
	}
	return &MassiveWSClient{
		url:      url,
		apiKey:   apiKey,
		symbols:  symbols,
		tickChan: make(chan Tick, bufferSize),
		stopChan: make(chan struct{}),
	}
}

// Ticks returns the receive-only channel of parsed Ticks.
func (c *MassiveWSClient) Ticks() <-chan Tick {
	return c.tickChan
}

func (c *MassiveWSClient) connectAndSubscribe(ctx context.Context) error {
	dialer := websocket.DefaultDialer
	conn, resp, err := dialer.DialContext(ctx, c.url, http.Header{})
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial massive ws (status %d): %w", resp.StatusCode, err)
		}
		return fmt.Errorf("dial massive ws: %w", err)
	}

	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = conn
	c.mu.Unlock()

	// 1. Send authentication message
	authPayload := map[string]string{
		"action": "auth",
		"params": c.apiKey,
	}
	if err := conn.WriteJSON(authPayload); err != nil {
		conn.Close()
		return fmt.Errorf("send auth: %w", err)
	}

	// 2. Await auth confirmation
	authConfirmed := false
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for !authConfirmed {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			conn.Close()
			return fmt.Errorf("read auth response: %w", err)
		}

		var events []RawMassiveEvent
		if err := json.Unmarshal(msg, &events); err == nil {
			for _, ev := range events {
				if ev.Ev == "status" {
					if ev.Status == "auth_success" {
						authConfirmed = true
						break
					} else if ev.Status == "auth_failed" {
						conn.Close()
						return errors.New("massive auth failed: invalid credentials")
					}
				}
			}
		}
	}
	conn.SetReadDeadline(time.Time{})

	// 3. Send subscription message for configured universe
	subStr := FormatSubscription(c.symbols)
	subPayload := map[string]string{
		"action": "subscribe",
		"params": subStr,
	}
	if err := conn.WriteJSON(subPayload); err != nil {
		conn.Close()
		return fmt.Errorf("send subscribe: %w", err)
	}

	log.Printf("[MASSIVE-WS] Authenticated. Subscribed to %d symbols: %s", len(c.symbols), subStr)
	return nil
}

// Start initiates the WebSocket connection, performs auth and subscription, and enters read loop.
func (c *MassiveWSClient) Start(ctx context.Context) error {
	if err := c.connectAndSubscribe(ctx); err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.readPump(ctx)
	}()
	return nil
}

func (c *MassiveWSClient) readPump(ctx context.Context) {
	defer func() {
		c.mu.Lock()
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()
		c.chanOnce.Do(func() {
			close(c.tickChan)
		})
	}()

	maxRetries := 10
	retryCount := 0

	for {
		select {
		case <-c.stopChan:
			return
		case <-ctx.Done():
			return
		default:
		}

		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()

		if conn == nil {
			return
		}

		_, msg, err := conn.ReadMessage()
		if err != nil {
			select {
			case <-c.stopChan:
				return
			case <-ctx.Done():
				return
			default:
				log.Printf("[MASSIVE-WS] Connection lost: %v. Initiating exponential backoff reconnect...", err)
			}

			// Exponential backoff reconnection loop
			reconnected := false
			delay := 500 * time.Millisecond
			maxDelay := 10 * time.Second

			for retryCount < maxRetries {
				jitter := time.Duration(rand.Intn(200)) * time.Millisecond
				totalDelay := delay + jitter

				select {
				case <-c.stopChan:
					return
				case <-ctx.Done():
					return
				case <-time.After(totalDelay):
				}

				retryCount++
				log.Printf("[MASSIVE-WS] Reconnect attempt %d/%d to %s...", retryCount, maxRetries, c.url)

				if err := c.connectAndSubscribe(ctx); err == nil {
					log.Printf("[MASSIVE-WS] Reconnection successful on attempt %d", retryCount)
					reconnected = true
					retryCount = 0
					break
				} else {
					log.Printf("[MASSIVE-WS] Reconnect attempt %d failed: %v", retryCount, err)
					delay *= 2
					if delay > maxDelay {
						delay = maxDelay
					}
				}
			}

			if !reconnected {
				log.Printf("[MASSIVE-WS] Permanent failure: exceeded %d reconnect attempts. Exiting.", maxRetries)
				return
			}
			continue
		}

		ticks, err := ParseMassiveEvents(msg)
		if err != nil {
			previewLen := len(msg)
			if previewLen > 120 {
				previewLen = 120
			}
			log.Printf("[MASSIVE-WS] Failed to parse events: %v (raw msg: %s)", err, string(msg[:previewLen]))
			continue
		}

		for _, t := range ticks {
			select {
			case c.tickChan <- t:
			case <-c.stopChan:
				return
			case <-ctx.Done():
				return
			}
		}
	}
}

// Close gracefully closes the WebSocket client.
func (c *MassiveWSClient) Close() error {
	c.closeOnce.Do(func() {
		close(c.stopChan)
		c.mu.Lock()
		if c.conn != nil {
			_ = c.conn.WriteControl(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
				time.Now().Add(time.Second),
			)
			_ = c.conn.Close()
			c.conn = nil
		}
		c.mu.Unlock()
		c.wg.Wait()
	})
	return nil
}
