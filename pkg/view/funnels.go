package view

import "log"

type Funnel struct {
	Steps []FunnelStep `json:"steps"`
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

type FunnelEvent struct {
	SessionId int
	TimeStamp int64
}

func (s *FunnelStep) AddEvent(evt FunnelEvent) {
	s.Events = append(s.Events, evt)
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
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				} else {
					log.Printf("[funnel] Event type mismatch: expected %d, got %d", filter.EventType, typedEvent.Event)
				}
			case ImpressionEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				} else {
					log.Printf("[funnel] ImpressionEvent type mismatch: expected %d, got %d", filter.EventType, typedEvent.Event)
				}
			case EnterCheckoutEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				} else {
					log.Printf("[funnel] EnterCheckoutEvent type mismatch: expected %d, got %d", filter.EventType, typedEvent.Event)
				}
			case CartEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				} else {
					log.Printf("[funnel] CartEvent type mismatch: expected %d, got %d", filter.EventType, typedEvent.Event)
				}

			case SearchEventData:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				} else {
					log.Printf("[funnel] SearchEventData type mismatch: expected %d, got %d", filter.EventType, typedEvent.Event)
				}
			case ActionEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				} else {
					log.Printf("[funnel] ActionEvent type mismatch: expected %d, got %d", filter.EventType, typedEvent.Event)
				}

			case SuggestEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				} else {
					log.Printf("[funnel] SuggestEvent type mismatch: expected %d, got %d", filter.EventType, typedEvent.Event)
				}
			case PurchaseEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				} else {
					log.Printf("[funnel] PurchaseEvent type mismatch: expected %d, got %d", filter.EventType, typedEvent.Event)
				}
			default:
				// Handle other event types if necessary
				log.Printf("[funnel] Unknown event type: %T", typedEvent)
			}
		}
	}
}
