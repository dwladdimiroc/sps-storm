package predictive

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	"io"
	"net/http"
	"strings"
)

type Response struct {
	AvgPrediction float64   `json:"avg_prediction"`
	Predictions   []float64 `json:"predictions"`
}

type PredictorData struct {
	Samples          []float64 `json:"samples"`
	PredictionNumber int       `json:"prediction_number"`
}

const PredictorURL = "http://PREDICTOR_HOST:PREDICTOR_PORT/PREDICTOR_MODEL"

func parseURL(urlRaw string) string {
	var url string
	predictorHost := viper.GetString("predictor.host")
	predictorPort := viper.GetString("predictor.port")
	predictorModel := viper.GetString("storm.adaptive.predictive_model")
	url = strings.Replace(urlRaw, "PREDICTOR_HOST", predictorHost, 1)
	url = strings.Replace(url, "PREDICTOR_PORT", predictorPort, 1)
	url = strings.Replace(url, "PREDICTOR_MODEL", predictorModel, 1)
	return url
}

func GetPrediction(samples []float64, predictionNumber int) []float64 {
	var resp Response

	var body = PredictorData{
		Samples:          samples,
		PredictionNumber: predictionNumber,
	}

	if b, err := json.Marshal(body); err != nil {
		fmt.Printf("storm get prediction: %v\n", err)
	} else {
		predictor := parseURL(PredictorURL)
		if res, err := http.Post(predictor, "application/json", bytes.NewBuffer(b)); err != nil {
			fmt.Printf("storm get prediction: %v\n", err)
		} else {
			data, _ := io.ReadAll(res.Body)
			if err := res.Body.Close(); err != nil {
				fmt.Printf("storm get prediction: %v\n", err)
			} else {
				if err := json.Unmarshal(data, &resp); err != nil {
					fmt.Printf("storm get prediction: %v\n", err)
				}
			}
		}
	}

	return resp.Predictions
}
