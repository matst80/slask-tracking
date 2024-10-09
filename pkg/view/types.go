package view

type BaseEvent struct {
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

type NumberSearch[K float64 | int] struct {
	Id  uint `json:"id"`
	Min K    `json:"min"`
	Max K    `json:"max"`
}

type StringSearch struct {
	Id    uint   `json:"id"`
	Value string `json:"value"`
}

type Filters struct {
	StringFilter  []StringSearch          `json:"string"`
	NumberFilter  []NumberSearch[float64] `json:"number"`
	IntegerFilter []NumberSearch[int]     `json:"integer"`
}

type SearchEventData struct {
	*BaseEvent
	*Filters
	Query string `json:"query"`
	Page  int    `json:"page"`
}

type PopularityListener interface {
	PopularityChanged(sort *SortOverride) error
}

type Impression struct {
	Id       uint    `json:"id"`
	Position float32 `json:"position"`
}

type ImpressionEvent struct {
	*BaseEvent
	Items []Impression `json:"items"`
}
