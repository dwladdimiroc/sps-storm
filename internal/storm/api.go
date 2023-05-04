package storm

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const NimbusSummaryTopologyBaseURL = "http://UI_HOST:UI_PORT/api/v1/topology/TOPOLOGY_ID?sys=0&windows=all-time"
const NimbusMetricsBaseURL = "http://UI_HOST:UI_PORT/api/v1/topology/TOPOLOGY_ID/metrics?window=all-time"
const NimbusSummaryTopologies = "http://UI_HOST:UI_PORT/api/v1/topology/summary"

func parseURL(urlRaw string, topologyId string) string {
	var url string
	nimbusHost := viper.GetString("nimbus.host")
	nimbusPort := viper.GetString("nimbus.port")
	url = strings.Replace(urlRaw, "UI_HOST", nimbusHost, 1)
	url = strings.Replace(url, "UI_PORT", nimbusPort, 1)
	url = strings.Replace(url, "TOPOLOGY_ID", topologyId, 1)
	return url
}

type SummaryTopologies struct {
	Topologies []struct {
		ID                  string  `json:"id"`
		Name                string  `json:"name"`
		Status              string  `json:"status"`
		Uptime              string  `json:"uptime"`
		UptimeSeconds       float64 `json:"uptimeSeconds"`
		TasksTotal          float64 `json:"tasksTotal"`
		WorkersTotal        float64 `json:"workersTotal"`
		ExecutorsTotal      float64 `json:"executorsTotal"`
		ReplicationCount    float64 `json:"replicationCount"`
		RequestedMemOnHeap  float64 `json:"requestedMemOnHeap"`
		RequestedMemOffHeap float64 `json:"requestedMemOffHeap"`
		RequestedTotalMem   float64 `json:"requestedTotalMem"`
		RequestedCPU        float64 `json:"requestedCpu"`
		AssignedMemOnHeap   float64 `json:"assignedMemOnHeap"`
		AssignedMemOffHeap  float64 `json:"assignedMemOffHeap"`
		AssignedTotalMem    float64 `json:"assignedTotalMem"`
		AssignedCPU         float64 `json:"assignedCpu"`
	} `json:"topologies"`
	SchedulerDisplayResource bool `json:"schedulerDisplayResource"`
}

func GetTopologyId() string {
	var summaryTopologies SummaryTopologies

	nimbusSummaryTopologies := parseURL(NimbusSummaryTopologies, "")
	if res, err := http.Get(nimbusSummaryTopologies); err != nil {
		fmt.Printf("storm get summary topologies: %v\n", err)
	} else {
		data, _ := io.ReadAll(res.Body)
		if err := res.Body.Close(); err != nil {
			fmt.Printf("storm get summary topologies: %v\n", err)
		} else {
			if err := json.Unmarshal(data, &summaryTopologies); err != nil {
				fmt.Printf("storm get summary topologies: %v\n", err)
			}
		}
	}

	if len(summaryTopologies.Topologies) > 0 {
		return summaryTopologies.Topologies[0].ID
	} else {
		time.Sleep(1 * time.Second)
		return GetTopologyId()
	}
}

type MetricsAPI struct {
	Window     string        `json:"window"`
	WindowHint string        `json:"window-hint"`
	Spouts     []SpoutMetric `json:"spouts"`
	Bolts      []BoltMetric  `json:"bolts"`
	Error      string        `json:"error"`
}

func (m *MetricsAPI) ParseValue() {
	for i := range m.Spouts {
		for j := range m.Spouts[i].CompleteMsAvg {
			m.Spouts[i].CompleteMsAvg[j].parseValue()
		}
	}

	for i := range m.Bolts {
		for j := range m.Bolts[i].ExecutedMsAvg {
			m.Bolts[i].ExecutedMsAvg[j].parseValue()
		}

		for j := range m.Bolts[i].ProcessMsAvg {
			m.Bolts[i].ProcessMsAvg[j].parseValue()
		}
	}
}

type SpoutMetric struct {
	ID            string       `json:"id"`
	Emitted       []Sender     `json:"emitted"`
	Transferred   []Sender     `json:"transferred"`
	Ack           []Channel    `json:"acked"`
	Failed        []Channel    `json:"failed"`
	CompleteMsAvg []ChannelAvg `json:"complete_ms_avg"`
}

type BoltMetric struct {
	ID            string       `json:"id"`
	Emitted       []Sender     `json:"emitted"`
	Transferred   []Sender     `json:"transferred"`
	Ack           []Channel    `json:"acked"`
	Failed        []Channel    `json:"failed"`
	ProcessMsAvg  []ChannelAvg `json:"process_ms_avg"`
	Executed      []Channel    `json:"executed"`
	ExecutedMsAvg []ChannelAvg `json:"executed_ms_avg"`
}

type Sender struct {
	StreamID string  `json:"stream_id"`
	Value    float64 `json:"value"`
}

type Channel struct {
	ComponentID string  `json:"component_id"`
	StreamID    string  `json:"stream_id"`
	Value       float64 `json:"value"`
}

type ChannelAvg struct {
	ComponentID string  `json:"component_id"`
	StreamID    string  `json:"stream_id"`
	Value       string  `json:"value"`
	ValueFloat  float64 `json:"value_float"`
}

func (c *ChannelAvg) parseValue() {
	c.ValueFloat, _ = strconv.ParseFloat(c.Value, 64)
}

func GetMetrics(topologyId string) (bool, MetricsAPI) {
	var metricsTopology MetricsAPI

	nimbusMetricsApiUrl := parseURL(NimbusMetricsBaseURL, topologyId)
	if res, err := http.Get(nimbusMetricsApiUrl); err != nil {
		fmt.Printf("storm get metrics: %v\n", err)
	} else {
		data, _ := io.ReadAll(res.Body)
		if err := res.Body.Close(); err != nil {
			fmt.Printf("storm get metrics: %v\n", err)
		} else {
			if err := json.Unmarshal(data, &metricsTopology); err != nil {
				fmt.Printf("storm get metrics: %v\n", err)
			} else {
				if metricsTopology.Error != "" {
					return false, metricsTopology
				}
			}
		}
	}

	metricsTopology.ParseValue()

	return true, metricsTopology
}

type SummaryTopology struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	Workers []struct {
		Host              string      `json:"host"`
		SupervisorID      string      `json:"supervisorId"`
		ComponentNumTasks interface{} `json:"componentNumTasks"`
	} `json:"workers"`
	Bolts []struct {
		BoltID string `json:"boltId"`
	} `json:"bolts"`
	Error string `json:"error"`
}

func GetSummaryTopology(topologyId string) SummaryTopology {
	var summaryTopology SummaryTopology

	nimbusSummaryTopologyURL := parseURL(NimbusSummaryTopologyBaseURL, topologyId)
	if res, err := http.Get(nimbusSummaryTopologyURL); err != nil {
		fmt.Printf("storm get summary topology: %v\n", err)
	} else {
		data, _ := io.ReadAll(res.Body)
		if err := res.Body.Close(); err != nil {
			fmt.Printf("storm get summary topology: %v\n", err)
		} else {
			if err := json.Unmarshal(data, &summaryTopology); err != nil {
				fmt.Printf("storm get summary topology: %v\n", err)
			}
		}
	}

	if len(summaryTopology.Bolts) > 0 {
		return summaryTopology
	} else {
		time.Sleep(1 * time.Second)
		return GetSummaryTopology(topologyId)
	}
}
