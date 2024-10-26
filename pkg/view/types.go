package view

import "github.com/matst80/slask-finder/pkg/index"

type BaseEvent struct {
	TimeStamp int64  `json:"ts"`
	SessionId uint32 `json:"session_id"`
	Event     uint16 `json:"event"`
}

type Session struct {
	*BaseEvent
	UserAgent    string `json:"user_agent,omitempty"`
	Ip           string `json:"ip,omitempty"`
	Language     string `json:"language,omitempty"`
	PragmaHeader string `json:"pragma,omitempty"`
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

type SearchEventData struct {
	*BaseEvent
	*index.Filters
	Query string `json:"query"`
	Page  int    `json:"page"`
}

type PopularityListener interface {
	PopularityChanged(sort *index.SortOverride) error
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
	Action string `json:"action"`
	Reason string `json:"reason"`
}
