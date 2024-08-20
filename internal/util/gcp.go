package util

import (
	"context"
	"errors"
	googlepb "github.com/golang/protobuf/ptypes/timestamp"
	"github.com/spf13/viper"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"log"
	"strings"
	"time"

	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
)

func GetCPU() float64 {
	ctx := context.Background()

	clientOptions := option.WithCredentialsFile(viper.GetString("gcp.auth_credential"))
	client, err := monitoring.NewMetricClient(ctx, clientOptions)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	interval := &monitoringpb.TimeInterval{
		EndTime: &googlepb.Timestamp{
			Seconds: time.Now().Unix(),
		},
		StartTime: &googlepb.Timestamp{
			Seconds: time.Now().Add(-10 * time.Minute).Unix(),
		},
	}

	req := &monitoringpb.ListTimeSeriesRequest{
		Name:     "projects/" + viper.GetString("gcp.project"),
		Filter:   `metric.type="compute.googleapis.com/instance/cpu/utilization"`,
		Interval: interval,
		View:     monitoringpb.ListTimeSeriesRequest_FULL,
	}

	var numVM int
	var cpuVM float64

	it := client.ListTimeSeries(ctx, req)
	for {
		resp, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			log.Fatalf("getCpu : error fetching time series: %v\n", err)
		}

		for _, vm := range resp.GetMetric().GetLabels() {
			if strings.Contains(vm, "supervisor") && !strings.Contains(vm, "template") {
				for _, point := range resp.Points {
					numVM++
					cpuVM += point.GetValue().GetDoubleValue()
					break
				}
			}
		}
	}

	return cpuVM / float64(numVM)
}
