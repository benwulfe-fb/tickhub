package main

const (
	// DefaultMetricsPort is the standard HTTP port for metrics and control endpoints.
	DefaultMetricsPort = "9090"
	// DefaultMetricsBind is the default listen address for the metrics HTTP server.
	DefaultMetricsBind = ":" + DefaultMetricsPort
	// DefaultControlAddr is the default connect address for feed CLI control subcommands.
	DefaultControlAddr = "127.0.0.1:" + DefaultMetricsPort
)
