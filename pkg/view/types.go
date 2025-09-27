package view

import (
	"strings"
	"time"

	"github.com/matst80/slask-finder/pkg/sorting"
	"github.com/matst80/slask-finder/pkg/types"
)

const (
	EVENT_SESSION_START = uint16(0)
	EVENT_ITEM_CLICK    = uint16(2)
	EVENT_ITEM_IMPRESS  = uint16(5)
	EVENT_ITEM_ACTION   = uint16(6)
	EVENT_SUGGEST       = uint16(7)
	EVENT_DATA_SET      = uint16(8)
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
	SessionId int64  `json:"session_id,omitempty"`
	Event     uint16 `json:"event"`
}

type DataSetEvent struct {
	*BaseEvent
	Query    string `json:"query"`
	Positive string `json:"positive,omitempty"`
	Negative string `json:"negative,omitempty"`
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

type BaseItem struct {
	Id        uint    `json:"id"`
	Position  float32 `json:"index"`
	Category  string  `json:"item_category,omitempty"`
	Category2 string  `json:"item_category2,omitempty"`
	Category3 string  `json:"item_category3,omitempty"`
	Category4 string  `json:"item_category4,omitempty"`
	Category5 string  `json:"item_category5,omitempty"`
	Brand     string  `json:"item_brand,omitempty"`
	Name      string  `json:"item_name,omitempty"`
	Price     float32 `json:"price,omitempty"`
	Quantity  uint    `json:"quantity,omitempty"`
}

//   item_name: string;
//   item_brand?: string;
//   item_category?: string;
//   item_category2?: string;
//   item_category3?: string;
//   item_category4?: string;
//   item_category5?: string;
//   item_list_id?: string;
//   item_list_name?: string;
//   index: number;
//   price?: number;

type Event struct {
	*BaseEvent
	*BaseItem
	//Referer  string  `json:"referer,omitempty"`
}

type EnterCheckoutEvent struct {
	*BaseEvent
	Items []BaseItem `json:"items"`
	//Referer  string  `json:"referer,omitempty"`
}

type CartEvent struct {
	*BaseEvent
	*BaseItem
	Type string `json:"type"`

	//Referer  string `json:"referer,omitempty"`
}

type Purchase struct {
	Id       uint `json:"item"`
	Quantity uint `json:"quantity"`
}

type PurchaseEvent struct {
	*BaseEvent
	Items []BaseItem `json:"items"`
	//Referer string     `json:"referer,omitempty"`
}

type SearchEvent struct {
	*BaseEvent
	*types.Filters
	NumberOfResults int    `json:"noi"`
	Query           string `json:"query,omitempty"`
	Page            int    `json:"page,omitempty"`
	//Referer         string `json:"referer,omitempty"`
}

type PopularityListener interface {
	PopularityChanged(sort *sorting.SortOverride) error
	FieldPopularityChanged(sort *sorting.SortOverride) error
	SessionPopularityChanged(sessionId int64, sort *sorting.SortOverride) error
	SessionFieldPopularityChanged(sessionId int64, sort *sorting.SortOverride) error
	GroupPopularityChanged(groupId string, sort *sorting.SortOverride) error
	GroupFieldPopularityChanged(groupId string, sort *sorting.SortOverride) error
}

type Impression struct {
	Id       uint    `json:"id"`
	Position float32 `json:"position"`
}

type ImpressionEvent struct {
	*BaseEvent
	Items []BaseItem `json:"items"`
}

type ActionEvent struct {
	*BaseEvent
	*BaseItem
	//Item    uint   `json:"id"`
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

func (e *Event) GetType() uint16 {
	return e.Event
}

func (e *SuggestEvent) GetType() uint16 {
	return e.Event
}

func (e *SearchEvent) GetType() uint16 {
	return e.Event
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

func (e *EnterCheckoutEvent) GetType() uint16 {
	return e.Event
}

func (e *Event) GetBaseEvent() *BaseEvent {
	return e.BaseEvent
}

func (e *SuggestEvent) GetBaseEvent() *BaseEvent {
	return e.BaseEvent
}

func (e *SearchEvent) GetBaseEvent() *BaseEvent {
	return e.BaseEvent
}

func (e *Session) GetBaseEvent() *BaseEvent {
	return e.BaseEvent
}

func (e *ActionEvent) GetBaseEvent() *BaseEvent {
	return e.BaseEvent
}

func (e *CartEvent) GetBaseEvent() *BaseEvent {
	return e.BaseEvent
}

func (e *ImpressionEvent) GetBaseEvent() *BaseEvent {
	return e.BaseEvent
}

func (e *PurchaseEvent) GetBaseEvent() *BaseEvent {
	return e.BaseEvent
}

func (e *EnterCheckoutEvent) GetBaseEvent() *BaseEvent {
	return e.BaseEvent
}

func (e *Event) GetTags() []string {
	return []string{}
}

func (e *SuggestEvent) GetTags() []string {
	return strings.Split(e.Value, " ")
}

func (e *SearchEvent) GetTags() []string {
	return []string{}
}

func (e *Session) GetTags() []string {
	return []string{}
}

func (e *ActionEvent) GetTags() []string {
	return []string{}
}

func (e *CartEvent) GetTags() []string {
	return []string{}
}

func (e *ImpressionEvent) GetTags() []string {
	return []string{}
}

func (e *PurchaseEvent) GetTags() []string {
	return []string{}
}

func (e *EnterCheckoutEvent) GetTags() []string {
	return []string{}
}

type TrackingEvent interface {
	GetType() uint16
	GetBaseEvent() *BaseEvent
	GetTags() []string
}
