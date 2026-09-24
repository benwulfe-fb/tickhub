package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	DefaultMassiveWSURL = "wss://socket.massive.com/stocks"
	NSPerMS             = 1_000_000
)

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
	P  float64 `json:"p,omitempty"`
	S  float64 `json:"s,omitempty"`
	Ds float64 `json:"ds,omitempty"`
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

// ParseMassiveEvents parses a JSON array of Massive.com events into Tick slice.
func ParseMassiveEvents(data []byte) ([]Tick, error) {
	var events []RawMassiveEvent
	if err := json.Unmarshal(data, &events); err != nil {
		return nil, fmt.Errorf("unmarshal massive events: %w", err)
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
			sz := ev.Ds
			if sz <= 0 {
				sz = ev.S
			}
			ticks = append(ticks, Tick{
				SIPTimestampNS: ev.T * NSPerMS,
				Symbol:         ev.Sym,
				Type:           TickTrade,
				Price:          ev.P,
				Size:           sz,
			})
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
