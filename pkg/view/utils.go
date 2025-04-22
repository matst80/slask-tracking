package view

import (
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

	return &SessionContent{
		Language:     r.Header.Get("Accept-Language"),
		UserAgent:    r.UserAgent(),
		Ip:           ip,
		PragmaHeader: r.Header.Get("Pragma"),
		Referrer:     r.Referer(),
	}
}
