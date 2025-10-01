package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/matst80/slask-tracking/pkg/view"
)

func TrackHandler(trk view.TrackingHandler, handler func(r *http.Request, sessionId int64, trackingHandler view.TrackingHandler) error) http.HandlerFunc {
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

func updateBaseEvent(r *http.Request, baseEvent *view.BaseEvent) *view.BaseEvent {
	country := "se"
	ref := r.Referer()
	if strings.Contains(ref, "-no") {
		country = "no"
	}
	if baseEvent.TimeStamp == 0 {
		baseEvent.TimeStamp = time.Now().Unix()
	}
	baseEvent.Country = country
	baseEvent.Context = "b2c"
	return baseEvent
}

func TrackClick(r *http.Request, sessionId int64, trk view.TrackingHandler) error {
	id := r.URL.Query().Get("id")
	itemId, err := strconv.Atoi(id)
	pos := r.URL.Query().Get("pos")
	position, _ := strconv.Atoi(pos)
	if err != nil {
		return err
	}

	go trk.HandleEvent(view.Event{
		BaseEvent: updateBaseEvent(r, &view.BaseEvent{Event: view.EVENT_ITEM_CLICK, SessionId: sessionId}),
		BaseItem: &view.BaseItem{
			Id:       uint(itemId),
			Position: float32(position),
		},
		//Referer:   referer,
	}, r)
	return nil
}

func TrackPostClick(r *http.Request, sessionId int64, trk view.TrackingHandler) error {
	clickData := &view.BaseItem{}
	err := json.NewDecoder(r.Body).Decode(clickData)
	if err != nil {
		return err
	}

	if clickData.Id == 0 {
		log.Printf("Click data is empty")
		return nil
	}
	go trk.HandleEvent(view.Event{
		BaseEvent: updateBaseEvent(r, &view.BaseEvent{Event: view.EVENT_ITEM_CLICK, SessionId: sessionId}),
		BaseItem:  clickData,
		//Referer:   referer,
	}, r)
	return nil
}

func TrackImpression(r *http.Request, sessionId int64, trk view.TrackingHandler) error {

	data := make([]view.BaseItem, 0)
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}
	go trk.HandleImpressionEvent(view.ImpressionEvent{
		BaseEvent: updateBaseEvent(r, &view.BaseEvent{Event: view.EVENT_ITEM_IMPRESS, SessionId: sessionId}),
		Items:     data,
	}, r)

	return nil
}

type ActionData struct {
	Item *view.BaseItem `json:"item,omitempty"`
	//Item   uint   `json:"item"`
	Action string `json:"action"`
	Reason string `json:"reason"`
}

type SuggestData struct {
	Value       string `json:"value"`
	Suggestions int    `json:"suggestions"`
	Results     int    `json:"results"`
}

func TrackAction(r *http.Request, sessionId int64, trk view.TrackingHandler) error {

	var data ActionData
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}
	referer := r.Header.Get("Referer")
	go trk.HandleActionEvent(view.ActionEvent{
		BaseEvent: updateBaseEvent(r, &view.BaseEvent{Event: view.EVENT_ITEM_ACTION, SessionId: sessionId}),
		BaseItem:  data.Item,
		Action:    data.Action,
		Reason:    data.Reason,
		Referer:   referer,
	}, r)

	return nil
}

func TrackSuggest(r *http.Request, sessionId int64, trk view.TrackingHandler) error {

	var data SuggestData
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}

	go trk.HandleSuggestEvent(view.SuggestEvent{
		BaseEvent:   updateBaseEvent(r, &view.BaseEvent{Event: view.EVENT_SUGGEST, SessionId: sessionId}),
		Value:       data.Value,
		Suggestions: data.Suggestions,
		Results:     data.Results,
		//Referer:     referer,
	}, r)

	return nil
}

type CartData struct {
	*view.BaseItem
	Type string `json:"type"`
}

func getCartEventType(cartType string) uint16 {
	switch cartType {
	case "add":
		return view.CART_ADD
	case "remove":
		return view.CART_REMOVE
	case "quantity":
		return view.CART_QUANTITY
	default:
		return view.CART_ADD
	}
}

type CheckoutData struct {
	Items []view.BaseItem `json:"items"`
}

func TrackCheckout(r *http.Request, sessionId int64, trk view.TrackingHandler) error {

	var data CheckoutData
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}

	go trk.HandleEnterCheckout(view.EnterCheckoutEvent{
		BaseEvent: updateBaseEvent(r, &view.BaseEvent{Event: view.CART_ENTER_CHECKOUT, SessionId: sessionId}),
		Items:     data.Items,

		//Referer:   referer,
	}, r)

	return nil
}

func TrackCart(r *http.Request, sessionId int64, trk view.TrackingHandler) error {

	var data CartData
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}

	eventType := getCartEventType(data.Type)

	go trk.HandleCartEvent(view.CartEvent{
		BaseEvent: updateBaseEvent(r, &view.BaseEvent{Event: eventType, SessionId: sessionId}),
		BaseItem:  data.BaseItem,
		Type:      data.Type,
		//Referer:   referer,
	}, r)

	return nil
}

type DataSet struct {
	Query    string `json:"query"`
	Positive string `json:"positive,omitempty"`
	Negative string `json:"negative,omitempty"`
}

func TrackDataSet(r *http.Request, sessionId int64, trk view.TrackingHandler) error {
	var data view.DataSetEvent
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		return err
	}

	go trk.HandleDataSetEvent(view.DataSetEvent{
		BaseEvent: updateBaseEvent(r, &view.BaseEvent{Event: view.EVENT_DATA_SET, SessionId: sessionId}),
		Query:     data.Query,
		Positive:  data.Positive,
		Negative:  data.Negative,
	}, r)

	return nil
}
