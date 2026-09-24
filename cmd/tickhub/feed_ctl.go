package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func runFeedCtl(args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: tickhub feed <status|enable|disable> [flags]\n")
		os.Exit(1)
	}

	cmd := args[0]
	fs := flag.NewFlagSet("feed "+cmd, flag.ExitOnError)
	controlAddr := fs.String("control-addr", DefaultControlAddr, "Address of tickhub daemon HTTP control server")
	timeout := fs.Duration("timeout", 5*time.Second, "HTTP request timeout")
	fs.Parse(args[1:])

	addr := *controlAddr
	if !strings.HasPrefix(addr, "http://") && !strings.HasPrefix(addr, "https://") {
		addr = "http://" + addr
	}

	client := &http.Client{Timeout: *timeout}

	switch cmd {
	case "status":
		resp, err := client.Get(addr + "/control/feed")
		if err != nil {
			log.Fatalf("[FEED-CTL] Request failed: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			log.Fatalf("[FEED-CTL] Error (status %d): %s", resp.StatusCode, string(body))
		}
		var st map[string]any
		_ = json.Unmarshal(body, &st)
		enabled, _ := st["enabled"].(bool)
		ticks, _ := st["ticks"].(float64)
		stateStr := "DISABLED (STANDBY)"
		if enabled {
			stateStr = "ENABLED (ACTIVE)"
		}
		fmt.Printf("TickHub Feed State: %s\nTotal Ingested Ticks: %d\n", stateStr, uint64(ticks))

	case "enable":
		resp, err := client.Post(addr+"/control/feed?action=enable", "application/json", nil)
		if err != nil {
			log.Fatalf("[FEED-CTL] Enable request failed: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			log.Fatalf("[FEED-CTL] Enable failed (status %d): %s", resp.StatusCode, string(body))
		}
		fmt.Printf("TickHub Feed successfully ENABLED.\n")

	case "disable":
		resp, err := client.Post(addr+"/control/feed?action=disable", "application/json", nil)
		if err != nil {
			log.Fatalf("[FEED-CTL] Disable request failed: %v", err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			log.Fatalf("[FEED-CTL] Disable failed (status %d): %s", resp.StatusCode, string(body))
		}
		fmt.Printf("TickHub Feed successfully DISABLED (Standby mode).\n")

	default:
		fmt.Fprintf(os.Stderr, "Unknown feed command: %s. Expected status, enable, or disable.\n", cmd)
		os.Exit(1)
	}
}
