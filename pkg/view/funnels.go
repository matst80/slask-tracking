package view

import (
	"fmt"
	"log"
)

type Funnel struct {
	Name  string                 `json:"name"`
	Steps map[string]*FunnelStep `json:"steps"`
}

type FunnelStep struct {
	Name          string         `json:"name"`
	SessionUnique bool           `json:"session_unique"`
	Sessions      map[int64]int  `json:"sessions,omitempty"`
	Filter        []FunnelFilter `json:"filter"`
	Events        []FunnelEvent  `json:"events"`
}

type Matcher string

const (
	MATCHER_NONE Matcher = "none"
	MATCHER_CART Matcher = "cart-event"
)

type FunnelFilter struct {
	Name      string  `json:"name"`
	EventType uint16  `json:"event_type"`
	MatchData string  `json:"match_data,omitempty"`
	Matcher   Matcher `json:"matcher,omitempty"`
}

const (
	FUNNEL_EVENT_ITEM_EVENT          = uint16(0)
	FUNNEL_EVENT_IMPRESSION          = uint16(1)
	FUNNEL_EVENT_ENTER_CHECKOUT      = uint16(2)
	FUNNEL_EVENT_CART_ADD            = uint16(3)
	FUNNEL_EVENT_CART_REMOVE         = uint16(4)
	FUNNEL_EVENT_CART_CLEAR          = uint16(5)
	FUNNEL_EVENT_CART_ENTER_CHECKOUT = uint16(6)
	FUNNEL_EVENT_SEARCH              = uint16(7)
	FUNNEL_EVENT_ACTION              = uint16(8)
	FUNNEL_EVENT_SUGGEST             = uint16(9)
	FUNNEL_EVENT_PURCHASE            = uint16(10)
)

type FunnelEvent struct {
	SessionId int64    `json:"session_id"`
	TimeStamp int64    `json:"ts,omitempty"`
	Tags      []string `json:"tags,omitempty"`
}

func (s *FunnelStep) AddEvent(evt FunnelEvent) {
	s.Events = append(s.Events, evt)
	//log.Printf("[funnel] Added event to step %s", s.Name)
}

func (s *FunnelStep) ClearEvents() {
	s.Events = []FunnelEvent{}
}

func (s *FunnelStep) ShouldHandle(base *BaseEvent, tags []string) bool {
	if s.Sessions == nil {
		s.Sessions = make(map[int64]int)
	}
	if s.SessionUnique && base.SessionId == 0 {
		return false
	}
	if s.SessionUnique {
		if _, ok := s.Sessions[base.SessionId]; ok {
			s.Sessions[base.SessionId]++
			return false
		}
	}
	return true
}

func (f *Funnel) ProcessEvent(evt TrackingEvent) {
	base := evt.GetBaseEvent()
	tags := evt.GetTags()

	for _, step := range f.Steps {
		for _, filter := range step.Filter {
			if filter.EventType == 0 {
				continue
			}
			if !step.ShouldHandle(base, tags) {
				continue
			}
			switch typedEvent := evt.(type) {
			case *Event:
				if FUNNEL_EVENT_ITEM_EVENT == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
						Tags:      typedEvent.GetTags(),
					})
				}
			case *ImpressionEvent:
				if FUNNEL_EVENT_IMPRESSION == filter.EventType {
					tags := make([]string, 0)
					for _, item := range typedEvent.Items {
						tags = append(tags, fmt.Sprintf("%d", item.Id))
					}
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
						Tags:      typedEvent.GetTags(),
					})
				}
			case *EnterCheckoutEvent:
				tags := make([]string, 0)
				for _, item := range typedEvent.Items {
					tags = append(tags, fmt.Sprintf("%d", item.Id))
				}
				if FUNNEL_EVENT_CART_ENTER_CHECKOUT == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
						Tags:      typedEvent.GetTags(),
					})
				}
			case *CartEvent:
				if FUNNEL_EVENT_CART_ADD == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
						Tags:      typedEvent.GetTags(),
					})
				}

			case *SearchEvent:
				if FUNNEL_EVENT_SEARCH == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
						Tags:      typedEvent.GetTags(),
					})
				}
			case *ActionEvent:
				if FUNNEL_EVENT_ACTION == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
						Tags:      typedEvent.GetTags(),
					})
				}

			case *SuggestEvent:
				if FUNNEL_EVENT_SUGGEST == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
						Tags:      typedEvent.GetTags(),
					})
				}
			case *PurchaseEvent:
				if FUNNEL_EVENT_PURCHASE == filter.EventType {

					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
						Tags:      typedEvent.GetTags(),
					})
				}
			default:
				// Handle other event types if necessary
				log.Printf("[funnel] Unknown event type: %T", typedEvent)
			}
		}
	}
}
