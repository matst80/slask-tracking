package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/matst80/slask-tracking/pkg/events"
	"github.com/matst80/slask-tracking/pkg/view"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var rabbitUrl = os.Getenv("RABBIT_URL")
var redisUrl = os.Getenv("REDIS_URL")
var redisPassword = os.Getenv("REDIS_PASSWORD")

func generateSessionId() int {
	return int(time.Now().UnixNano())
}

func setSessionCookie(w http.ResponseWriter, r *http.Request, session_id int) {
	http.SetCookie(w, &http.Cookie{
		Name:   "sid",
		Value:  fmt.Sprintf("%d", session_id),
		Domain: strings.TrimPrefix(r.Host, "."),
		MaxAge: 2592000000,
		Path:   "/", //MaxAge: 7200
	})
}

func HandleSessionCookie(h view.TrackingHandler, w http.ResponseWriter, r *http.Request) int {
	sessionId := generateSessionId()
	c, err := r.Cookie("sid")
	if err != nil {
		// fmt.Printf("Failed to get cookie %v", err)
		if h != nil {
			ip := r.Header.Get("X-Real-Ip")
			if ip == "" {
				ip = r.Header.Get("X-Forwarded-For")
			}
			if ip == "" {
				ip = r.RemoteAddr
			}

			go h.HandleSessionEvent(view.Session{
				BaseEvent:    &view.BaseEvent{Event: view.EVENT_SESSION_START, SessionId: uint32(sessionId)},
				Language:     r.Header.Get("Accept-Language"),
				UserAgent:    r.UserAgent(),
				Ip:           ip,
				PragmaHeader: r.Header.Get("Pragma"),
			})
		}
		setSessionCookie(w, r, sessionId)

	} else {
		sessionId, err = strconv.Atoi(c.Value)
		if err != nil {
			setSessionCookie(w, r, sessionId)
		}
	}
	return sessionId
}

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
	trackServer := WebServer{
		Tracking: viewHandler,
	}
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
	mux.HandleFunc("/tracking/my/session", func(w http.ResponseWriter, r *http.Request) {
		sessionId := HandleSessionCookie(viewHandler, w, r)
		session := viewHandler.GetSession(sessionId)
		if session != nil {
			w.Header().Set("Content-Type", "application/json")
			err := json.NewEncoder(w).Encode(session)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		} else {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("No session found"))
		}
	})
	mux.HandleFunc("/track/click", trackServer.TrackClick)
	mux.HandleFunc("/track/impressions", trackServer.TrackImpression)
	mux.HandleFunc("/track/action", trackServer.TrackAction)
	mux.HandleFunc("/track/cart", trackServer.TrackCart)
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
