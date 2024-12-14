package view

import "github.com/matst80/slask-finder/pkg/index"

const (
	EVENT_SESSION_START = uint16(0)
	EVENT_ITEM_CLICK    = uint16(2)
	EVENT_ITEM_IMPRESS  = uint16(5)
	EVENT_ITEM_ACTION   = uint16(6)
	EVENT_SEARCH        = uint16(1)
)

const (
	CART_ADD            = uint16(11)
	CART_REMOVE         = uint16(12)
	CART_CLEAR          = uint16(13)
	CART_ENTER_CHECKOUT = uint16(14)
)

type BaseEvent struct {
	TimeStamp int64  `json:"ts"`
	SessionId int    `json:"session_id"`
	Event     uint16 `json:"event"`
}

type SessionContent struct {
	UserAgent    string `json:"user_agent,omitempty"`
	Ip           string `json:"ip,omitempty"`
	Language     string `json:"language,omitempty"`
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
}

type CartEvent struct {
	*BaseEvent
	Item     uint `json:"item"`
	Quantity uint `json:"quantity"`
}

type Purchase struct {
	Id       uint `json:"item"`
	Quantity uint `json:"quantity"`
}

type PurchaseEvent struct {
	*BaseEvent
	Items []Purchase `json:"items"`
}

type SearchEventData struct {
	*BaseEvent
	*index.Filters
	Query string `json:"query"`
	Page  int    `json:"page"`
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
	Item   uint   `json:"id"`
	Action string `json:"action"`
	Reason string `json:"reason"`
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
