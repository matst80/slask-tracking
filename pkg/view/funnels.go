package view

import "log"

type Funnel struct {
	Name  string                 `json:"name"`
	Steps map[string]*FunnelStep `json:"steps"`
}

type FunnelStep struct {
	Name   string         `json:"name"`
	Filter []FunnelFilter `json:"filter"`
	Events []FunnelEvent  `json:"events"`
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
	SessionId int
	TimeStamp int64
}

func (s *FunnelStep) AddEvent(evt FunnelEvent) {
	s.Events = append(s.Events, evt)
	log.Printf("[funnel] Added event to step %s", s.Name)
}

func (s *FunnelStep) ClearEvents() {
	s.Events = []FunnelEvent{}
}

func (f *Funnel) ProcessEvent(evt interface{}) {
	for _, step := range f.Steps {
		for _, filter := range step.Filter {
			if filter.EventType == 0 {
				continue
			}
			switch typedEvent := evt.(type) {
			case Event:
				if FUNNEL_EVENT_ITEM_EVENT == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}
			case ImpressionEvent:
				if FUNNEL_EVENT_IMPRESSION == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}
			case EnterCheckoutEvent:
				if FUNNEL_EVENT_CART_ENTER_CHECKOUT == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}
			case CartEvent:
				if FUNNEL_EVENT_CART_ADD == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}

			case SearchEventData:
				if FUNNEL_EVENT_SEARCH == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}
			case ActionEvent:
				if FUNNEL_EVENT_ACTION == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}

			case SuggestEvent:
				if FUNNEL_EVENT_SUGGEST == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}
			case PurchaseEvent:
				if FUNNEL_EVENT_PURCHASE == filter.EventType {
					step.AddEvent(FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}
			default:
				// Handle other event types if necessary
				log.Printf("[funnel] Unknown event type: %T", typedEvent)
			}
		}
	}
}
