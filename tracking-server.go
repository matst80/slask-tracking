package main

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/matst80/slask-tracking/pkg/view"
)

func TrackHandler(trk view.TrackingHandler, handler func(r *http.Request, sessionId int, trackingHandler view.TrackingHandler) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			w.Header().Set("Cache-Control", "public, max-age=3600")
			origin := r.Header.Get("Origin")
			if origin != "" && !strings.Contains(origin, "localhost") {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Max-Age", "86400")
				w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "*")
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
			w.Header().Set("Age", "0")
		} else {
			w.Header().Set("Cache-Control", "private, stale-while-revalidate=5")
			if trk == nil {
				http.Error(w, "Tracking not enabled", http.StatusNotImplemented)
				return
			}
			sessionId := HandleSessionCookie(trk, w, r)
			err := handler(r, sessionId, trk)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func TrackClick(r *http.Request, sessionId int, trk view.TrackingHandler) error {
	id := r.URL.Query().Get("id")
	itemId, err := strconv.Atoi(id)
	pos := r.URL.Query().Get("pos")
	position, _ := strconv.Atoi(pos)
	referer := r.Header.Get("Referer")
	if err != nil {
		return err
	}

	go trk.HandleEvent(view.Event{
		BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_CLICK, SessionId: sessionId, TimeStamp: time.Now().Unix()},
		Item:      uint(itemId),
		Position:  float32(position) / 100.0,
		Referer:   referer,
	})
	return nil
}

func TrackImpression(r *http.Request, sessionId int, trk view.TrackingHandler) error {

	data := make([]view.Impression, 0)
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}
	go trk.HandleImpressionEvent(view.ImpressionEvent{
		BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_IMPRESS, SessionId: sessionId, TimeStamp: time.Now().Unix()},
		Items:     data,
	})

	return nil
}

type ActionData struct {
	Item   uint   `json:"item"`
	Action string `json:"action"`
	Reason string `json:"reason"`
}

func TrackAction(r *http.Request, sessionId int, trk view.TrackingHandler) error {

	var data ActionData
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}
	referer := r.Header.Get("Referer")
	go trk.HandleActionEvent(view.ActionEvent{
		BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_ACTION, SessionId: sessionId, TimeStamp: time.Now().Unix()},
		Item:      data.Item,
		Action:    data.Action,
		Reason:    data.Reason,
		Referer:   referer,
	})

	return nil
}

type CartData struct {
	Item     uint `json:"item"`
	Quantity uint `json:"quantity"`
}

func TrackCart(r *http.Request, sessionId int, trk view.TrackingHandler) error {

	var data CartData
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}
	referer := r.Header.Get("Referer")
	go trk.HandleCartEvent(view.CartEvent{
		BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_ACTION, SessionId: sessionId, TimeStamp: time.Now().Unix()},
		Item:      data.Item,
		Quantity:  data.Quantity,
		Referer:   referer,
	})

	return nil
}
