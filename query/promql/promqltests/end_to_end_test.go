package promqltests_test

import (
	"testing"

	_ "github.com/influxdata/influxdb/query/builtin"
	"github.com/influxdata/promqltests"
)

func TestPromQLEndToEnd(t *testing.T) {
	engine, err := promqltests.NewTestEngine(promqltests.InputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	for _, q := range promqltests.TestCases {
		t.Run(q.Text, func(t *testing.T) {
			if len(q.Skip) > 0 {
				t.Skip(q.Skip)
			}
			engine.Test(t, q.Text, q.SkipComparison, q.ShouldFail, promqltests.Start, promqltests.End, promqltests.Resolution)
		})
	}
}
