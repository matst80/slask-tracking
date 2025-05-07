package view

import (
	"log"
	"net/http"
)

func GetSessionContentFromRequest(r *http.Request) *SessionContent {
	if r == nil {
		return nil
	}
	ip := r.Header.Get("X-Real-Ip")
	if ip == "" {
		ip = r.Header.Get("X-Forwarded-For")
	}
	if ip == "" {
		ip = r.RemoteAddr
	}
	log.Printf("session update ip:%s, user_agent:%s, referrer: %s", ip, r.UserAgent(), r.Referer())
	return &SessionContent{
		Language:     r.Header.Get("Accept-Language"),
		UserAgent:    r.UserAgent(),
		Ip:           ip,
		PragmaHeader: r.Header.Get("Pragma"),
		Referrer:     r.Referer(),
	}
}
