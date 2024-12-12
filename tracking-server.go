package main

import (
	"encoding/json"
	"github.com/matst80/slask-tracking/pkg/view"
	"net/http"
	"strconv"
)

type WebServer struct {
	Tracking view.TrackingHandler
}

func (ws *WebServer) TrackClick(w http.ResponseWriter, r *http.Request) {
	sessionId := HandleSessionCookie(ws.Tracking, w, r)
	id := r.URL.Query().Get("id")
	itemId, err := strconv.Atoi(id)
	pos := r.URL.Query().Get("pos")
	position, _ := strconv.Atoi(pos)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if ws.Tracking != nil {
		go ws.Tracking.HandleEvent(view.Event{
			BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_CLICK, SessionId: uint32(sessionId)},
			Item:      uint(itemId),
			Position:  float32(position) / 100.0,
		})
	}

	w.WriteHeader(http.StatusAccepted)
}

func (ws *WebServer) TrackImpression(w http.ResponseWriter, r *http.Request) {
	sessionId := HandleSessionCookie(ws.Tracking, w, r)
	data := make([]view.Impression, 0)
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if ws.Tracking != nil {
		ws.Tracking.HandleImpressionEvent(view.ImpressionEvent{
			BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_IMPRESS, SessionId: uint32(sessionId)},
			Items:     data,
		})
	}

	w.WriteHeader(http.StatusAccepted)
}

type ActionData struct {
	Item   uint   `json:"item"`
	Action string `json:"action"`
	Reason string `json:"reason"`
}

func (ws *WebServer) TrackAction(w http.ResponseWriter, r *http.Request) {
	sessionId := HandleSessionCookie(ws.Tracking, w, r)
	var data ActionData
	err := json.NewDecoder(r.Body).Decode(&data)

	if ws.Tracking != nil && err == nil {

		ws.Tracking.HandleActionEvent(view.ActionEvent{
			BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_ACTION, SessionId: uint32(sessionId)},
			Item:      data.Item,
			Action:    data.Action,
			Reason:    data.Reason,
		})

	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

type CartData struct {
	Item     uint `json:"item"`
	Quantity uint `json:"quantity"`
}

func (ws *WebServer) TrackCart(w http.ResponseWriter, r *http.Request) {
	sessionId := HandleSessionCookie(ws.Tracking, w, r)
	var data CartData
	err := json.NewDecoder(r.Body).Decode(&data)

	if ws.Tracking != nil && err == nil {

		ws.Tracking.HandleCartEvent(view.CartEvent{
			BaseEvent: &view.BaseEvent{Event: view.EVENT_ITEM_ACTION, SessionId: uint32(sessionId)},
			Item:      data.Item,
			Quantity:  data.Quantity,
		})

	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
