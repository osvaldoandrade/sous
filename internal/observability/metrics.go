package observability

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func MetricsHandler(reg *prometheus.Registry) http.Handler {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
