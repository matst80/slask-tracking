package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"

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
			TrackingTopic: "tracking",
			//ItemsUpsertedTopic: "item_added",
			Url:   rabbitUrl,
			VHost: os.Getenv("RABBIT_HOST"),
		},
	}
	viewHandler := view.MakeMemoryTrackingHandler("data/tracking.json", 500)
	popularityHandler := view.NewSortOverrideStorage(redisUrl, redisPassword, 0)

	defer viewHandler.Save()
	go client.Connect(viewHandler)
	// go client.ConnectUpdates(viewHandler)
	// go client.ConnectPriceUpdates(viewHandler)
	viewHandler.ConnectPopularityListener(popularityHandler)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/tracking/save", func(w http.ResponseWriter, r *http.Request) {
		viewHandler.Save()
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/tracking/my/session", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		sessionId := HandleSessionCookie(viewHandler, w, r)
		return viewHandler.GetSession(sessionId), nil
	}))
	mux.HandleFunc("/tracking/session/{id}", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		id := r.PathValue("id")
		sessionId, err := strconv.Atoi(id)
		if err != nil {
			return nil, err
		}
		return viewHandler.GetSession(sessionId), nil
	}))
	mux.HandleFunc("/track/click", TrackHandler(viewHandler, TrackClick))
	mux.HandleFunc("/track/impressions", TrackHandler(viewHandler, TrackImpression))
	mux.HandleFunc("/track/action", TrackHandler(viewHandler, TrackAction))
	mux.HandleFunc("/track/suggest", TrackHandler(viewHandler, TrackSuggest))
	mux.HandleFunc("/track/cart", TrackHandler(viewHandler, TrackCart))
	mux.HandleFunc("/track/enter-checkout", TrackHandler(viewHandler, TrackCheckout))
	mux.HandleFunc("GET /tracking/suggest", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		q := r.URL.Query().Get("q")
		return viewHandler.GetSuggestions(q), nil
	}))
	mux.HandleFunc("GET /tracking/funnels", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetFunnels()
	}))
	mux.HandleFunc("PUT /tracking/funnels", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		var funnels []view.Funnel
		err := json.NewDecoder(r.Body).Decode(&funnels)
		if err != nil {
			return nil, err
		}
		err = viewHandler.SetFunnels(funnels)
		if err != nil {
			return nil, err
		}
		return viewHandler.GetFunnels()
	}))
	mux.HandleFunc("GET /tracking/popularity", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetItemPopularity(), nil
	}))
	mux.HandleFunc("GET /tracking/field-popularity", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetFieldPopularity(), nil
	}))

	mux.HandleFunc("GET /tracking/field-popularity/{id}", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		idString := r.PathValue("id")
		id, err := strconv.Atoi(idString)
		if err != nil {
			return nil, err
		}
		return viewHandler.GetFieldValuePopularity(uint(id)), nil
	}))

	mux.HandleFunc("GET /tracking/queries", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetQueries(), nil
	}))
	// mux.HandleFunc("/tracking/updated", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
	// 	return viewHandler.GetUpdatedItems(), nil
	// }))
	mux.HandleFunc("GET /tracking/sessions", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetSessions(), nil
	}))
	mux.Handle("/metrics", promhttp.Handler())
	log.Println("Starting server on port 8080")
	err := http.ListenAndServe(":8080", mux)
	if err != nil {
		return 1
	}
	return 0
}

func main() {
	os.Exit(run_application())
}
