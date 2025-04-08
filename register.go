// Package influxdb registers the xk6-influxdb extension
package influxdb

import (
	"github.com/shadmanakbar/xk6-output-influxdb/pkg/influxdb"
	"go.k6.io/k6/output"
)

func init() {
	output.RegisterExtension("xk6-influxdb", func(p output.Params) (output.Output, error) {
		return influxdb.New(p)
	})
}
