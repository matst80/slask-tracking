package view

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

type ActionEvent struct {
	*BaseEvent
	Action string `json:"action"`
	Reason string `json:"reason"`
}

type PriceUpdateItem struct {
	Url             string  `json:"url"`
	Id              uint    `json:"id"`
	Disclaimer      string  `json:"disclaimer,omitempty"`
	ReleaseDate     string  `json:"releaseDate,omitempty"`
	SaleStatus      string  `json:"saleStatus"`
	MarginPercent   float64 `json:"mp,omitempty"`
	PresaleDate     string  `json:"presaleDate,omitempty"`
	Restock         string  `json:"restock,omitempty"`
	AdvertisingText string  `json:"advertisingText,omitempty"`
	Img             string  `json:"img,omitempty"`
	BadgeUrl        string  `json:"badgeUrl,omitempty"`
	BulletPoints    string  `json:"bp,omitempty"`
	LastUpdate      int64   `json:"lastUpdate,omitempty"`
	Created         int64   `json:"created,omitempty"`
	Buyable         bool    `json:"buyable"`
	BuyableInStore  bool    `json:"buyableInStore"`
}
