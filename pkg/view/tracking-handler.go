package view

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/matst80/slask-finder/pkg/index"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type TrackingHandler interface {
	HandleSessionEvent(event Session)
	HandleEvent(event Event, r *http.Request)
	//UpdateSessionFromRequest(sessionId int, r *http.Request)
	HandleSearchEvent(event SearchEvent, r *http.Request)
	HandleCartEvent(event CartEvent, r *http.Request)
	HandleEnterCheckout(event EnterCheckoutEvent, r *http.Request)
	HandleImpressionEvent(event ImpressionEvent, r *http.Request)
	HandleActionEvent(event ActionEvent, r *http.Request)
	HandleSuggestEvent(event SuggestEvent, r *http.Request)
	GetSession(sessionId int) *SessionData
}

// type UpdateHandler interface {
// 	HandleUpdate(update []interface{})
// }

// type PriceUpdateHandler interface {
// 	HandlePriceUpdate(update []index.DataItem)
// }

type FacetValueResult struct {
	Value string  `json:"value"`
	Score float64 `json:"score"`
}

type FacetResult struct {
	FacetId uint               `json:"id"`
	Score   float64            `json:"score"`
	Values  []FacetValueResult `json:"values"`
}

type QueryResult struct {
	Query  string        `json:"query"`
	Score  float64       `json:"score"`
	Facets []FacetResult `json:"facets"`
}

type QueryKeyData struct {
	FieldPopularity *DecayPopularity            `json:"popularity"`
	ValuePopularity map[string]*DecayPopularity `json:"values"`
}

type QueryMatcher struct {
	Popularity *DecayPopularity `json:"popularity"`
	//	Query      string                `json:"query"`
	KeyFields map[uint]QueryKeyData `json:"keyFacets"`
}

func (q *QueryMatcher) AddKeyFilterEvent(key uint, value string) {
	ts := time.Now().Unix()
	popularity, ok := q.KeyFields[key]
	if !ok {
		popularity = QueryKeyData{
			FieldPopularity: &DecayPopularity{},
			ValuePopularity: make(map[string]*DecayPopularity),
		}
		q.KeyFields[key] = popularity
	}
	popularity.FieldPopularity.Add(DecayEvent{
		TimeStamp: ts,
		Value:     100,
	})
	if value != "" {
		valuePopularity, ok := popularity.ValuePopularity[value]
		if !ok {
			valuePopularity = &DecayPopularity{}
			popularity.ValuePopularity[value] = valuePopularity
		}
		valuePopularity.Add(DecayEvent{
			TimeStamp: ts,
			Value:     100,
		})
	}

}

type PersistentMemoryTrackingHandler struct {
	path             string
	mu               sync.RWMutex
	changes          uint
	updatesToKeep    int
	trackingHandler  PopularityListener
	FieldValueScores map[uint][]FacetValueResult          `json:"field_value_scores"`
	ItemPopularity   index.SortOverride                   `json:"item_popularity"`
	Queries          map[string]uint                      `json:"queries"`
	QueryEvents      map[string]QueryMatcher              `json:"suggestions"`
	Sessions         map[int]*SessionData                 `json:"sessions"`
	FieldPopularity  index.SortOverride                   `json:"field_popularity"`
	ItemEvents       DecayList                            `json:"item_events"`
	FieldEvents      DecayList                            `json:"field_events"`
	SortedQueries    []QueryResult                        `json:"sorted_queries"`
	FieldValueEvents map[uint]map[string]*DecayPopularity `json:"field_value_events"`
	Funnels          []Funnel                             `json:"funnel_storage"`
	EmptyResults     []SearchEvent                        `json:"empty_results"`
	//UpdatedItems    []interface{}        `json:"updated_items"`
}

type SessionData struct {
	*SessionContent
	ItemPopularity  index.SortOverride `json:"item_popularity"`
	FieldPopularity index.SortOverride `json:"field_popularity"`
	Id              int                `json:"id"`
	Events          []interface{}      `json:"events"`
	ItemEvents      DecayList          `json:"item_events"`
	FieldEvents     DecayList          `json:"field_events"`
	Created         int64              `json:"ts"`
	LastUpdate      int64              `json:"last_update"`
	LastSync        int64              `json:"last_sync"`
}

const (
	eventLimit = 500
)

func (session *SessionData) HandleEvent(event interface{}) {
	if session.ItemEvents == nil {
		session.ItemEvents = make(map[uint][]DecayEvent)
	}
	if session.FieldEvents == nil {
		session.FieldEvents = make(map[uint][]DecayEvent)
	}
	if session.Events == nil {
		session.Events = make([]interface{}, 0)
	}
	start := max(0, len(session.Events)-eventLimit)
	session.Events = append(session.Events[start:], event)
	ts := time.Now().Unix()
	now := ts

	session.LastUpdate = now
	switch e := event.(type) {
	case Event:
		if e.BaseItem != nil && e.Id > 0 {
			session.ItemEvents.Add(e.Id, DecayEvent{
				TimeStamp: now,
				Value:     200,
			})
		} else {
			log.Printf("Event without item %+v", event)
		}
		return
	case SearchEvent:
		for _, filter := range e.Filters.StringFilter {
			session.FieldEvents.Add(filter.Id, DecayEvent{
				TimeStamp: now,
				Value:     150,
			})
		}
		for _, filter := range e.Filters.RangeFilter {
			session.FieldEvents.Add(filter.Id, DecayEvent{
				TimeStamp: now,
				Value:     100,
			})
		}
		return
	case ImpressionEvent:
		for _, impression := range e.Items {
			session.ItemEvents.Add(impression.Id, DecayEvent{
				TimeStamp: now,
				Value:     0.02 * float64(max(impression.Position, 300)),
			})
		}
		return
	case CartEvent:
		session.ItemEvents.Add(e.Id, DecayEvent{
			TimeStamp: now,
			Value:     700,
		})
		return
	case ActionEvent:
		if e.BaseItem != nil && e.Id > 0 {
			session.ItemEvents.Add(e.Id, DecayEvent{
				TimeStamp: now,
				Value:     80,
			})
		}
		return
	case PurchaseEvent:
		for _, purchase := range e.Items {
			session.ItemEvents.Add(purchase.Id, DecayEvent{
				TimeStamp: now,
				Value:     800 * float64(purchase.Quantity),
			})
		}
		return
	case SuggestEvent:
		return
	default:
		log.Printf("Unknown event type %T", event)
	}

}

var (
	opsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slasktracking_processed_tracking_events_total",
		Help: "The total number of processed tracking events",
	})
	// updatedProcessed = promauto.NewCounter(prometheus.CounterOpts{
	// 	Name: "slasktracking_processed_update_events_total",
	// 	Help: "The total number of processed update events",
	// })
	sessions_total = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slasktracking_sessions_total",
		Help: "The total number sessions",
	})
)

func (s *PersistentMemoryTrackingHandler) ConnectPopularityListener(handler PopularityListener) {
	s.trackingHandler = handler
}

func MakeMemoryTrackingHandler(path string, itemsToKeep int) *PersistentMemoryTrackingHandler {
	instance, err := load(path)
	if err != nil {
		instance = &PersistentMemoryTrackingHandler{
			path:             "data",
			mu:               sync.RWMutex{},
			changes:          0,
			updatesToKeep:    0,
			trackingHandler:  nil,
			EmptyResults:     make([]SearchEvent, 0),
			QueryEvents:      make(map[string]QueryMatcher),
			ItemPopularity:   make(index.SortOverride),
			Queries:          make(map[string]uint),
			Sessions:         make(map[int]*SessionData),
			FieldPopularity:  make(index.SortOverride),
			ItemEvents:       map[uint][]DecayEvent{},
			FieldEvents:      map[uint][]DecayEvent{},
			FieldValueEvents: make(map[uint]map[string]*DecayPopularity),
			Funnels:          make([]Funnel, 0),
			SortedQueries:    make([]QueryResult, 0),
			//UpdatedItems:    make([]interface{}, 0),
		}
	}
	go func() {
		for range time.Tick(time.Minute) {
			if instance.changes > 0 {
				err := instance.save()
				if err != nil {
					log.Println(err)
				}
			}
		}
	}()

	instance.path = path
	instance.changes = 0
	instance.updatesToKeep = itemsToKeep
	if instance.ItemPopularity == nil {
		instance.ItemPopularity = make(index.SortOverride)
	}
	if instance.Queries == nil {
		instance.Queries = make(map[string]uint)
	}
	if instance.Sessions == nil {
		instance.Sessions = make(map[int]*SessionData)
	}
	if instance.FieldPopularity == nil {
		instance.FieldPopularity = make(index.SortOverride)
	}
	if instance.Funnels == nil || len(instance.Funnels) == 0 {
		instance.Funnels = []Funnel{
			{

				Steps: map[string]*FunnelStep{
					"view": {

						Name: "See item",
						Filter: []FunnelFilter{
							{
								Name:      "Impression",
								EventType: FUNNEL_EVENT_IMPRESSION,
								Matcher:   MATCHER_NONE,
							},
						},
						Events: make([]FunnelEvent, 0),
					},
					"cart": {
						Name: "Add to cart",
						Filter: []FunnelFilter{
							{
								Name:      "Add to cart",
								EventType: FUNNEL_EVENT_CART_ADD,
								Matcher:   MATCHER_CART,
							},
						},
						Events: make([]FunnelEvent, 0),
					},
				},
			},
		}
	}
	// if instance.UpdatedItems == nil {
	// 	instance.UpdatedItems = make([]interface{}, 0)
	// }
	return instance
}

func (s *PersistentMemoryTrackingHandler) Save() {
	s.save()
}

func (s *PersistentMemoryTrackingHandler) save() error {
	s.DecaySuggestions()
	s.DecayEvents()

	s.DecaySessionEvents()
	s.cleanSessions()
	s.DecayFacetValuesEvents()

	defer runtime.GC()
	if s.changes == 0 {
		return nil
	}
	if s.trackingHandler != nil {
		go s.trackingHandler.PopularityChanged(&s.ItemPopularity)
		go s.trackingHandler.FieldPopularityChanged(&s.FieldPopularity)
	}

	log.Println("Saving tracking data")

	s.changes = 0
	err := s.writeFile(s.path)

	return err
}

func load(path string) (*PersistentMemoryTrackingHandler, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	result := &PersistentMemoryTrackingHandler{}

	err = json.NewDecoder(file).Decode(result)
	if err == nil {
		if result.QueryEvents == nil {
			result.QueryEvents = make(map[string]QueryMatcher)
		}
		if result.FieldValueEvents == nil {
			result.FieldValueEvents = make(map[uint]map[string]*DecayPopularity)
		}
	}

	return result, err
}

func (s *PersistentMemoryTrackingHandler) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changes++
	s.Sessions = make(map[int]*SessionData)
	//s.ItemPopularity = make(index.SortOverride)
	//s.Queries = make(map[string]uint)
	//s.QueryEvents = make(map[string]QueryMatcher)
	//s.FieldPopularity = make(index.SortOverride)
	s.ItemEvents = map[uint][]DecayEvent{}
	s.FieldEvents = map[uint][]DecayEvent{}
	s.EmptyResults = make([]SearchEvent, 0)
	//s.FieldValueEvents = make(map[uint]map[string]*DecayPopularity)
	//s.UpdatedItems = make([]interface{}, 0)

}

func (s *PersistentMemoryTrackingHandler) GetSession(sessionId int) *SessionData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	session, ok := s.Sessions[sessionId]
	if ok {
		return session
	}
	return nil
}

func (s *PersistentMemoryTrackingHandler) writeFile(path string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	err = json.NewEncoder(file).Encode(s)
	return err
}

func (s *PersistentMemoryTrackingHandler) GetFunnels() ([]Funnel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Funnels, nil
}

func (s *PersistentMemoryTrackingHandler) SetFunnels(funnels []Funnel) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changes++
	s.Funnels = funnels
	return nil
}

func (s *PersistentMemoryTrackingHandler) GetItemEvents() DecayList {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ItemEvents
}

func (s *PersistentMemoryTrackingHandler) GetItemPopularity() map[uint]float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ItemPopularity
}

func (s *PersistentMemoryTrackingHandler) GetSuggestions(q string) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if q == "" {
		return s.SortedQueries
	}
	lq := strings.TrimSpace(strings.ToLower(q))

	matching := make([]QueryResult, 0)
	for _, query := range s.SortedQueries {
		if strings.Contains(query.Query, lq) {
			matching = append(matching, query)
		}
	}
	if len(matching) > 0 {
		return matching
	}
	return []QueryResult{}
}

func (s *PersistentMemoryTrackingHandler) GetQueries() map[string]uint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Queries
}

func (s *PersistentMemoryTrackingHandler) GetNoResultQueries() []SearchEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.EmptyResults
}

func (s *PersistentMemoryTrackingHandler) GetSessions() []*SessionData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sessions := make([]*SessionData, len(s.Sessions))
	i := 0
	for _, session := range s.Sessions {
		if len(session.Events) > 1 {
			sessions[i] = session
			i++
		}
	}
	return sessions[:i]
}

func (s *PersistentMemoryTrackingHandler) GetFieldPopularity() index.SortOverride {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.FieldPopularity
}

func (s *PersistentMemoryTrackingHandler) GetFieldValuePopularity(id uint) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values, ok := s.FieldValueScores[id]
	if !ok {
		return nil
	}
	return values
}

func (s *PersistentMemoryTrackingHandler) HandleSessionEvent(event Session) {
	// log.Printf("Session new session event %d", event.SessionId)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changes++
	opsProcessed.Inc()

	events := make([]interface{}, 0)
	s.Sessions[event.SessionId] = &SessionData{
		SessionContent: &event.SessionContent,
		Created:        time.Now().Unix(),
		LastUpdate:     time.Now().Unix(),
		Events:         events,
		ItemEvents:     make(map[uint][]DecayEvent),
		FieldEvents:    make(map[uint][]DecayEvent),
	}
}

func (s *PersistentMemoryTrackingHandler) HandleEvent(event Event, r *http.Request) {
	// log.Printf("Event SessionId: %d, ItemId: %d, Position: %f", event.SessionId, event.Item, event.Position)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ItemEvents.Add(event.Id, DecayEvent{
		TimeStamp: time.Now().Unix(),
		Value:     200.0 + (0.1 * float64(min(event.Position, 300))),
	})

	go s.handleFunnels(&event)
	s.updateSession(event, event.SessionId, r)

	s.changes++
	go opsProcessed.Inc()
}

func (s *PersistentMemoryTrackingHandler) handleFunnels(event TrackingEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, funnel := range s.Funnels {
		funnel.ProcessEvent(event)
	}
}

func (s *PersistentMemoryTrackingHandler) HandleEnterCheckout(event EnterCheckoutEvent, r *http.Request) {
	// log.Printf("EnterCheckout event SessionId: %d, ItemId: %d, Quantity: %d", event.SessionId, event.Item, event.Quantity)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, item := range event.Items {
		s.ItemEvents.Add(item.Id, DecayEvent{
			TimeStamp: time.Now().Unix(),
			Value:     200.0 * float64(item.Quantity),
		})
	}
	s.changes++
	go opsProcessed.Inc()
	go s.handleFunnels(&event)
	s.updateSession(event, event.SessionId, r)
}

func (s *PersistentMemoryTrackingHandler) HandleCartEvent(event CartEvent, r *http.Request) {
	// log.Printf("Cart event SessionId: %d, ItemId: %d, Quantity: %d", event.SessionId, event.Item, event.Quantity)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ItemEvents.Add(event.Id, DecayEvent{
		TimeStamp: time.Now().Unix(),
		Value:     190.0 * float64(event.Quantity),
	})
	s.changes++
	go opsProcessed.Inc()
	go s.handleFunnels(&event)
	s.updateSession(event, event.SessionId, r)
}

func normalizeQuery(query string) string {
	query = strings.ToLower(query)
	query = strings.TrimSpace(query)
	return query
}

func (s *PersistentMemoryTrackingHandler) UpdateSessionFromRequest(sessionId int, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	session, ok := s.Sessions[sessionId]
	if ok {
		session.Ip = r.RemoteAddr
		s.Sessions[sessionId] = session
	}

}

func (s *PersistentMemoryTrackingHandler) HandleSearchEvent(event SearchEvent, r *http.Request) {
	if event.NumberOfResults == 0 {
		if s.EmptyResults == nil {
			s.EmptyResults = make([]SearchEvent, 0)
		}
		if event.Query != "" {
			s.EmptyResults = append(s.EmptyResults, event)
			log.Printf("Search event with no results %+v", event)
		}

		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changes++
	go opsProcessed.Inc()
	ts := time.Now().Unix()

	if event.Query != "" && event.Query != "*" {
		normalizedQuery := normalizeQuery(event.Query)
		s.Queries[normalizedQuery] += 1

		if normalizedQuery != "" {
			queryEvents, ok := s.QueryEvents[normalizedQuery]
			if !ok {
				queryEvents = QueryMatcher{
					//Query:      event.Query,
					Popularity: &DecayPopularity{},
					KeyFields:  make(map[uint]QueryKeyData),
				}
				s.QueryEvents[normalizedQuery] = queryEvents
			}
			queryEvents.Popularity.Add(DecayEvent{
				TimeStamp: ts,
				Value:     20.0, // + (float64(event.NumberOfResults) * 0.5),
			})
			//queryEvents.Popularity.Decay(ts)
			for _, filter := range event.Filters.StringFilter {
				switch v := filter.Value.(type) {
				case string:
					queryEvents.AddKeyFilterEvent(filter.Id, v)
				case []string:
					for _, value := range v {
						queryEvents.AddKeyFilterEvent(filter.Id, value)
					}
				case []interface{}:
					for _, value := range v {
						if strValue, ok := value.(string); ok {
							queryEvents.AddKeyFilterEvent(filter.Id, strValue)
						}
					}
				default:
					log.Printf("Unknown type %T for filter %d", filter.Value, filter.Id)
				}
			}
		}
	} else {

		for _, filter := range event.Filters.StringFilter {
			s.FieldEvents.Add(filter.Id, DecayEvent{
				TimeStamp: ts,
				Value:     40.0,
			})
			for _, filter := range event.Filters.StringFilter {
				fieldValues, ok := s.FieldValueEvents[filter.Id]
				if !ok {
					fieldValues = make(map[string]*DecayPopularity)
					s.FieldValueEvents[filter.Id] = fieldValues
				}
				addFieldValueEvent := func(value string) {
					fieldPopularity, ok := fieldValues[value]
					if !ok {
						fieldPopularity = &DecayPopularity{}
						fieldValues[value] = fieldPopularity
					}
					fieldPopularity.Add(DecayEvent{
						TimeStamp: ts,
						Value:     80,
					})
				}
				switch v := filter.Value.(type) {
				case string:
					addFieldValueEvent(v)
				case []string:
					for _, value := range v {
						addFieldValueEvent(value)
					}
				default:
					log.Printf("Unknown type %T for filter %d", filter.Value, filter.Id)
				}

			}
		}
		for _, filter := range event.Filters.RangeFilter {
			s.FieldEvents.Add(filter.Id, DecayEvent{
				TimeStamp: ts,
				Value:     30,
			})
		}
	}

	go s.handleFunnels(&event)
	s.updateSession(event, event.SessionId, r)

}

func (s *PersistentMemoryTrackingHandler) updateSession(event interface{}, sessionId int, r *http.Request) {

	session, ok := s.Sessions[sessionId]
	now := time.Now().Unix()
	if !ok {
		sessions_total.Inc()
		session = &SessionData{
			SessionContent:  GetSessionContentFromRequest(r),
			Created:         now,
			LastUpdate:      now,
			LastSync:        0,
			Id:              sessionId,
			Events:          make([]interface{}, 1),
			ItemEvents:      make(map[uint][]DecayEvent),
			FieldEvents:     make(map[uint][]DecayEvent),
			ItemPopularity:  make(index.SortOverride),
			FieldPopularity: make(index.SortOverride),
		}
		s.Sessions[sessionId] = session
	} else {

		session.LastUpdate = now
		if r != nil {
			session.SessionContent = GetSessionContentFromRequest(r)
		}
	}

	session.HandleEvent(event)

}

func (s *PersistentMemoryTrackingHandler) HandleImpressionEvent(event ImpressionEvent, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	go opsProcessed.Inc()
	for _, impression := range event.Items {
		s.ItemEvents.Add(impression.Id, DecayEvent{
			TimeStamp: time.Now().Unix(),
			Value:     float64(impression.Position),
		})
		//s.ItemPopularity[impression.Id] += 5.01 + float64(impression.Position)/10
	}
	s.updateSession(event, event.SessionId, r)
	go s.handleFunnels(&event)
	s.changes++

}

func (s *PersistentMemoryTrackingHandler) HandleActionEvent(event ActionEvent, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	go opsProcessed.Inc()
	if event.BaseItem != nil && event.Id > 0 {
		s.ItemEvents.Add(event.Id, DecayEvent{
			TimeStamp: time.Now().Unix(),
			Value:     30,
		})
	}
	s.updateSession(event, event.SessionId, r)
	go s.handleFunnels(&event)
	s.changes++
}

func (s *PersistentMemoryTrackingHandler) HandleSuggestEvent(event SuggestEvent, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	go opsProcessed.Inc()
	s.updateSession(event, event.SessionId, r)
	go s.handleFunnels(&event)
	s.Queries[event.Value] += 1
	// TODO update this to somethign useful
	// TODO add decay to this
	//log.Printf("Suggest %s", event.Value)
	s.changes++
}
