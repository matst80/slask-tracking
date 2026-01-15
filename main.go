package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/matst80/slask-finder/pkg/messaging"
	"github.com/matst80/slask-tracking/pkg/events"
	"github.com/matst80/slask-tracking/pkg/view"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	amqp "github.com/rabbitmq/amqp091-go"
)

var rabbitUrl = os.Getenv("RABBIT_URL")
var country = "no"

func init() {
	if rabbitUrl == "" {
		log.Fatalf("RABBIT_URL environment variable is not set")
	}
}

func run_application() int {

	conn, err := amqp.DialConfig(rabbitUrl, amqp.Config{
		Properties: amqp.NewConnectionProperties(),
	})
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	err = messaging.DefineTopic(ch, "global", "sort_override")
	if err != nil {
		log.Fatalf("Failed to define topic: %v", err)
	}

	viewHandler := view.MakeMemoryTrackingHandler("data/tracking.json", 500)
	popularityHandler := view.NewSortOverrideStorage(conn)

	if cfg, ok := loadClickhouseConfigFromEnv(); ok {
		chHandler, err := view.NewClickhouseTrackingHandler(context.Background(), cfg)
		if err != nil {
			log.Printf("clickhouse logging disabled: %v", err)
		} else {
			viewHandler.AttachFollower(chHandler)
			defer chHandler.Close()
			log.Printf("clickhouse logging enabled for %s.%s", cfg.Database, cfg.EventsTable)
		}
	}

	defer viewHandler.Save()
	go func() {
		err := events.ConnectTrackingHandler(ch, viewHandler)
		if err != nil {
			log.Printf("Failed to connect tracking handler: %v", err)
		}
	}()

	viewHandler.ConnectPopularityListener(popularityHandler)
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/tracking/save", func(w http.ResponseWriter, r *http.Request) {
		viewHandler.Save()
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("/tracking/variation/{id}", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		id := r.PathValue("id")
		sessionId := HandleSessionCookie(viewHandler, w, r)
		session := viewHandler.GetSession(sessionId)
		if session == nil {
			return nil, nil
		}
		return session.HandleVariation(id)
	}))

	mux.HandleFunc("/tracking/my/groups", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		sessionId := HandleSessionCookie(viewHandler, w, r)
		session := viewHandler.GetSession(sessionId)
		if session == nil {
			return nil, nil
		}

		groups := session.Groups
		if len(groups) > 0 {
			groupValues := make([]string, 0)
			for id := range groups {
				groupValues = append(groupValues, id)
			}
			http.SetCookie(w, &http.Cookie{
				Name: "persona", Value: strings.Join(groupValues, ","),
				HttpOnly: true,
				Secure:   true,
				SameSite: http.SameSiteNoneMode,
				Domain:   strings.TrimPrefix(r.Host, "."),
				MaxAge:   2592000000,
				Path:     "/",
			})
		}

		return session.Groups, nil
	}))
	mux.HandleFunc("/tracking/my/session", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		sessionId := HandleSessionCookie(viewHandler, w, r)
		return viewHandler.GetSession(sessionId), nil
	}))
	mux.HandleFunc("/tracking/session/{id}", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		id := r.PathValue("id")
		sessionId, err := strconv.ParseInt(id, 10, 64)
		log.Printf("fetching session id: %d", sessionId)
		if err != nil {
			return nil, err
		}
		return viewHandler.GetSession(sessionId), nil
	}))
	mux.HandleFunc("GET /track/click", TrackHandler(viewHandler, TrackClick))
	mux.HandleFunc("POST /track/click", TrackHandler(viewHandler, TrackPostClick))
	mux.HandleFunc("/track/impressions", TrackHandler(viewHandler, TrackImpression))
	mux.HandleFunc("/track/action", TrackHandler(viewHandler, TrackAction))
	mux.HandleFunc("/track/suggest", TrackHandler(viewHandler, TrackSuggest))
	mux.HandleFunc("/track/cart", TrackHandler(viewHandler, TrackCart))
	mux.HandleFunc("/track/dataset", TrackHandler(viewHandler, TrackDataSet))
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
	mux.HandleFunc("GET /tracking/item-events", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetItemEvents(), nil
	}))
	mux.HandleFunc("GET /tracking/popularity", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetItemPopularity(), nil
	}))
	mux.HandleFunc("GET /tracking/field-popularity", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetFieldPopularity(), nil
	}))
	mux.HandleFunc("GET /tracking/dataset", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetDataSet(), nil
	}))

	mux.HandleFunc("GET /tracking/clear", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		viewHandler.Clear()
		return true, nil
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
	mux.HandleFunc("GET /tracking/no-results", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetNoResultQueries(), nil
	}))
	// mux.HandleFunc("/tracking/updated", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
	// 	return viewHandler.GetUpdatedItems(), nil
	// }))
	mux.HandleFunc("GET /tracking/sessions", JsonHandler(func(w http.ResponseWriter, r *http.Request) (interface{}, error) {
		return viewHandler.GetSessions(), nil
	}))
	mux.Handle("/metrics", promhttp.Handler())
	log.Println("Starting server on port 8080")
	err = http.ListenAndServe(":8080", mux)
	if err != nil {
		return 1
	}
	return 0
}

func main() {
	os.Exit(run_application())
}

func loadClickhouseConfigFromEnv() (view.ClickhouseConfig, bool) {
	addressesEnv := os.Getenv("CLICKHOUSE_ADDR")
	if addressesEnv == "" {
		addressesEnv = os.Getenv("CLICKHOUSE_ADDRESSES")
	}
	addresses := splitAndNormaliseAddresses(addressesEnv)
	if len(addresses) == 0 {
		return view.ClickhouseConfig{}, false
	}

	cfg := view.ClickhouseConfig{
		Addresses:     addresses,
		Database:      getenvDefault("CLICKHOUSE_DATABASE", "tracking"),
		Username:      os.Getenv("CLICKHOUSE_USERNAME"),
		Password:      os.Getenv("CLICKHOUSE_PASSWORD"),
		EventsTable:   getenvDefault("CLICKHOUSE_EVENTS_TABLE", "events"),
		SessionsTable: getenvDefault("CLICKHOUSE_SESSIONS_TABLE", "sessions"),
		Secure:        parseBoolEnv("CLICKHOUSE_SECURE"),
		SkipVerifyTLS: parseBoolEnv("CLICKHOUSE_INSECURE_SKIP_VERIFY"),
	}

	if d := parseDurationEnv("CLICKHOUSE_DIAL_TIMEOUT"); d > 0 {
		cfg.DialTimeout = d
	}
	if d := parseDurationEnv("CLICKHOUSE_READ_TIMEOUT"); d > 0 {
		cfg.ReadTimeout = d
	}
	if d := parseDurationEnv("CLICKHOUSE_WRITE_TIMEOUT"); d > 0 {
		cfg.WriteTimeout = d
	}

	return cfg, true
}

func splitAndNormaliseAddresses(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.FieldsFunc(value, func(r rune) bool {
		return r == ',' || r == ' ' || r == ';'
	})
	addresses := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "//") && !strings.Contains(part, ":") {
			part = part + ":9000"
		}
		addresses = append(addresses, part)
	}
	return addresses
}

func getenvDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}

func parseBoolEnv(key string) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	switch value {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func parseDurationEnv(key string) time.Duration {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return 0
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		log.Printf("invalid duration for %s: %v", key, err)
		return 0
	}
	return duration
}
