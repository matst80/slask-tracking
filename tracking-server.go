package main

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/matst80/slask-tracking/pkg/view"
)

func TrackHandler(trk view.TrackingHandler, handler func(w http.ResponseWriter, r *http.Request, sessionId int, trackingHandler view.TrackingHandler) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			w.Header().Set("Cache-Control", "public, max-age=3600")
			origin := r.Header.Get("Origin")
			if origin != "" {
				w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
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
			err := handler(w, r, sessionId, trk)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		w.WriteHeader(http.StatusAccepted)
	}
}

func TrackClick(w http.ResponseWriter, r *http.Request, sessionId int, trk view.TrackingHandler) error {
	id := r.URL.Query().Get("id")
	itemId, err := strconv.Atoi(id)
	pos := r.URL.Query().Get("pos")
	position, _ := strconv.Atoi(pos)
	if err != nil {
		return err
	}

	go trk.HandleEvent(view.Event{
		BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_CLICK, SessionId: sessionId},
		Item:      uint(itemId),
		Position:  float32(position) / 100.0,
	})
	return nil
}

func TrackImpression(w http.ResponseWriter, r *http.Request, sessionId int, trk view.TrackingHandler) error {

	data := make([]view.Impression, 0)
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}
	go trk.HandleImpressionEvent(view.ImpressionEvent{
		BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_IMPRESS, SessionId: sessionId},
		Items:     data,
	})

	return nil
}

type ActionData struct {
	Item   uint   `json:"item"`
	Action string `json:"action"`
	Reason string `json:"reason"`
}

func TrackAction(w http.ResponseWriter, r *http.Request, sessionId int, trk view.TrackingHandler) error {

	var data ActionData
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}
	go trk.HandleActionEvent(view.ActionEvent{
		BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_ACTION, SessionId: sessionId},
		Item:      data.Item,
		Action:    data.Action,
		Reason:    data.Reason,
	})

	return nil
}

type CartData struct {
	Item     uint `json:"item"`
	Quantity uint `json:"quantity"`
}

func TrackCart(w http.ResponseWriter, r *http.Request, sessionId int, trk view.TrackingHandler) error {

	var data CartData
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}
	go trk.HandleCartEvent(view.CartEvent{
		BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_ACTION, SessionId: sessionId},
		Item:      data.Item,
		Quantity:  data.Quantity,
	})

	return nil
}
