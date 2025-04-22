package view

import (
	"time"

	"github.com/matst80/slask-finder/pkg/index"
	"github.com/matst80/slask-finder/pkg/types"
)

const (
	EVENT_SESSION_START = uint16(0)
	EVENT_ITEM_CLICK    = uint16(2)
	EVENT_ITEM_IMPRESS  = uint16(5)
	EVENT_ITEM_ACTION   = uint16(6)
	EVENT_SUGGEST       = uint16(7)
	EVENT_SEARCH        = uint16(1)
)

const (
	CART_ADD            = uint16(11)
	CART_REMOVE         = uint16(12)
	CART_CLEAR          = uint16(13)
	CART_ENTER_CHECKOUT = uint16(14)
	CART_QUANTITY       = uint16(15)
)

type BaseEvent struct {
	TimeStamp int64  `json:"ts,omitempty"`
	SessionId int    `json:"session_id,omitempty"`
	Event     uint16 `json:"event"`
}

func (e *BaseEvent) SetTimestamp() {
	if e.TimeStamp == 0 {
		e.TimeStamp = time.Now().Unix()
	}
}

type SessionContent struct {
	UserAgent    string `json:"user_agent,omitempty"`
	Ip           string `json:"ip,omitempty"`
	Language     string `json:"language,omitempty"`
	Referrer     string `json:"referrer,omitempty"`
	PragmaHeader string `json:"pragma,omitempty"`
}

type Session struct {
	*BaseEvent
	SessionContent
}

type Event struct {
	*BaseEvent
	Item     uint    `json:"item"`
	Position float32 `json:"position"`
	//Referer  string  `json:"referer,omitempty"`
}

type EnterCheckoutEvent struct {
	*BaseEvent
	Items []Purchase `json:"items"`
	//Referer  string  `json:"referer,omitempty"`
}

type CartEvent struct {
	*BaseEvent

	Type     string `json:"type"`
	Item     uint   `json:"item"`
	Quantity uint   `json:"quantity"`
	//Referer  string `json:"referer,omitempty"`
}

type Purchase struct {
	Id       uint `json:"item"`
	Quantity uint `json:"quantity"`
}

type PurchaseEvent struct {
	*BaseEvent
	Items []Purchase `json:"items"`
	//Referer string     `json:"referer,omitempty"`
}

type SearchEventData struct {
	*BaseEvent
	*types.Filters
	NumberOfResults int    `json:"noi"`
	Query           string `json:"query,omitempty"`
	Page            int    `json:"page,omitempty"`
	//Referer         string `json:"referer,omitempty"`
}

type PopularityListener interface {
	PopularityChanged(sort *index.SortOverride) error
	FieldPopularityChanged(sort *index.SortOverride) error
	SessionPopularityChanged(sessionId int, sort *index.SortOverride) error
	SessionFieldPopularityChanged(sessionId int, sort *index.SortOverride) error
}

type Impression struct {
	Id       uint    `json:"id"`
	Position float32 `json:"position"`
}

type ImpressionEvent struct {
	*BaseEvent
	Items []Impression `json:"items"`
}

type ActionEvent struct {
	*BaseEvent
	Item    uint   `json:"id"`
	Action  string `json:"action"`
	Reason  string `json:"reason"`
	Referer string `json:"referer,omitempty"`
}

type SuggestEvent struct {
	*BaseEvent
	Value       string `json:"value,omitempty"`
	Suggestions int    `json:"suggestions,omitempty"`
	Results     int    `json:"results,omitempty"`
	//Referer     string `json:"referer,omitempty"`
}

func (e *Session) GetType() uint16 {
	return e.Event
}

func (e *ActionEvent) GetType() uint16 {
	return e.Event
}

func (e *CartEvent) GetType() uint16 {
	return e.Event
}

func (e *ImpressionEvent) GetType() uint16 {
	return e.Event
}

func (e *PurchaseEvent) GetType() uint16 {
	return e.Event
}

type TrackingEvent interface {
	GetType() uint16
}
