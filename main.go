package main

import (
	"encoding/json"
	"net/http"
	"os"

	"github.com/matst80/slask-tracking/pkg/events"
	"github.com/matst80/slask-tracking/pkg/view"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var rabbitUrl = os.Getenv("RABBIT_URL")
var redisUrl = os.Getenv("REDIS_URL")
var redisPassword = os.Getenv("REDIS_PASSWORD")

func run_application() int {
	client := events.RabbitTransportClient{
		RabbitTrackingConfig: events.RabbitTrackingConfig{
			TrackingTopic:      "tracking",
			ItemsUpsertedTopic: "item_added",
			Url:                rabbitUrl,
		},
	}
	viewHandler := view.MakeMemoryTrackingHandler("data/tracking.json", 500)
	popularityHandler := view.NewSortOverrideStorage(redisUrl, redisPassword, 0)
	defer viewHandler.Save()
	go client.Connect(viewHandler)
	go client.ConnectUpdates(viewHandler)
	go client.ConnectPriceUpdates(viewHandler)
	viewHandler.ConnectPopularityListener(popularityHandler)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/tracking/save", func(w http.ResponseWriter, r *http.Request) {
		viewHandler.Save()
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/tracking/popularity", func(w http.ResponseWriter, r *http.Request) {
		result := viewHandler.GetItemPopularity()
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/tracking/field-popularity", func(w http.ResponseWriter, r *http.Request) {
		result := viewHandler.GetFieldPopularity()
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/tracking/queries", func(w http.ResponseWriter, r *http.Request) {
		result := viewHandler.GetQueries()
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/tracking/updated", func(w http.ResponseWriter, r *http.Request) {
		result := viewHandler.GetUpdatedItems()
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/tracking/sessions", func(w http.ResponseWriter, r *http.Request) {
		result := viewHandler.GetSessions()
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":8080", mux)
	if err != nil {
		return 1
	}
	return 0
}

func main() {
	os.Exit(run_application())
}
