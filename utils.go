package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/matst80/slask-tracking/pkg/view"
)

func generateSessionId() int {
	return int(time.Now().UnixNano())
}

func setSessionCookie(w http.ResponseWriter, r *http.Request, session_id int) {
	http.SetCookie(w, &http.Cookie{
		Name:     "sid",
		Value:    fmt.Sprintf("%d", session_id),
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteNoneMode,
		Domain:   strings.TrimPrefix(r.Host, "."),
		MaxAge:   2592000000,
		Path:     "/", //MaxAge: 7200
	})
}

func HandleSessionCookie(h view.TrackingHandler, w http.ResponseWriter, r *http.Request) int {
	sessionId := generateSessionId()
	c, err := r.Cookie("sid")
	if err != nil {
		// fmt.Printf("Failed to get cookie %v", err)
		if h != nil {

			go h.HandleSessionEvent(view.Session{
				BaseEvent:      &view.BaseEvent{Event: view.EVENT_SESSION_START, SessionId: sessionId},
				SessionContent: *view.GetSessionContentFromRequest(r),
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

func JsonHandler(handler func(w http.ResponseWriter, r *http.Request) (interface{}, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result, err := handler(w, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if result == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")

		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
		}
		if r.Method == http.MethodGet {
			w.Header().Set("Age", "0")
			w.Header().Set("Cache-Control", "public, stale-while-revalidate=60")
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(result)
	}
}
