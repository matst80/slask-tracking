package view

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
				}
			case ImpressionEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}
			case CartEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}

			case SearchEventData:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}
			case ActionEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}

			case SuggestEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}
			case PurchaseEvent:
				if typedEvent.Event == filter.EventType {
					step.Events = append(step.Events, FunnelEvent{
						SessionId: typedEvent.SessionId,
						TimeStamp: typedEvent.TimeStamp,
					})
				}
			}
		}
	}
}
