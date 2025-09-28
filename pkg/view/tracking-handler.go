package view

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/matst80/slask-finder/pkg/sorting"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type TrackingHandler interface {
	HandleSessionEvent(event Session)
	HandleEvent(event Event, r *http.Request)
	HandleSearchEvent(event SearchEvent, r *http.Request)
	HandleCartEvent(event CartEvent, r *http.Request)
	HandleDataSetEvent(event DataSetEvent, r *http.Request)
	HandleEnterCheckout(event EnterCheckoutEvent, r *http.Request)
	HandleImpressionEvent(event ImpressionEvent, r *http.Request)
	HandleActionEvent(event ActionEvent, r *http.Request)
	HandleSuggestEvent(event SuggestEvent, r *http.Request)
	GetSession(sessionId int64) *SessionData
}

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

type ProductRelation struct {
	ItemId uint               `json:"item_id"`
	Other  map[uint]DecayList `json:"other"`
}

type PersistentMemoryTrackingHandler struct {
	path                  string
	mu                    sync.RWMutex
	changes               uint
	updatesToKeep         int
	trackingHandler       PopularityListener
	ViewedTogether        map[uint]ProductRelation             `json:"viewed_together"`
	AlsoBought            map[uint]ProductRelation             `json:"also_bought"`
	DataSet               []DataSetEvent                       `json:"dataset"`
	FieldValueScores      map[uint][]FacetValueResult          `json:"field_value_scores"`
	ItemPopularity        sorting.SortOverride                 `json:"item_popularity"`
	Queries               map[string]uint                      `json:"queries"`
	QueryEvents           map[string]QueryMatcher              `json:"suggestions"`
	Sessions              map[int64]*SessionData               `json:"sessions"`
	FieldPopularity       sorting.SortOverride                 `json:"field_popularity"`
	ItemEvents            DecayList                            `json:"item_events"`
	FieldEvents           DecayList                            `json:"field_events"`
	SortedQueries         []QueryResult                        `json:"sorted_queries"`
	FieldValueEvents      map[uint]map[string]*DecayPopularity `json:"field_value_events"`
	Funnels               []Funnel                             `json:"funnel_storage"`
	EmptyResults          []SearchEvent                        `json:"empty_results_v2"`
	PersonalizationGroups map[string]PersonalizationGroup      `json:"personalization_groups"`
	//UpdatedItems    []interface{}        `json:"updated_items"`
}

type SessionData struct {
	*SessionContent
	VisitedSkus []uint                 `json:"visited_skus"`
	Groups      map[string]float64     `json:"groups"`
	Variations  map[string]interface{} `json:"variations"`
	// ItemPopularity  index.SortOverride     `json:"item_popularity"`
	// FieldPopularity index.SortOverride     `json:"field_popularity"`
	Id          int64         `json:"id"`
	Events      []interface{} `json:"events"`
	ItemEvents  DecayList     `json:"item_events"`
	FieldEvents DecayList     `json:"field_events"`
	Created     int64         `json:"ts"`
	LastUpdate  int64         `json:"last_update"`
	LastSync    int64         `json:"last_sync"`
}

func (session *SessionData) HandleVariation(id string) (interface{}, error) {
	if session.Variations == nil {
		session.Variations = make(map[string]interface{})
	}
	if v, ok := session.Variations[id]; ok {
		return v, nil
	}
	var ret interface{}
	v := rand.IntN(100)
	if v < 50 {
		ret = "a"
	} else {
		ret = "b"
	}
	session.Variations[id] = ret
	return ret, nil

}

const (
	eventLimit = 500
)

func (session *SessionData) HandleEvent(event interface{}) map[string]float64 {
	if session.ItemEvents == nil {
		session.ItemEvents = make(map[uint][]DecayEvent)
	}
	if session.FieldEvents == nil {
		session.FieldEvents = make(map[uint][]DecayEvent)
	}
	if session.VisitedSkus == nil {
		session.VisitedSkus = make([]uint, 0)
	}
	if session.Events == nil {
		log.Printf("make new event-list, %s", session.Id)
		session.Events = make([]interface{}, 0)
	}
	if session.Groups == nil {
		session.Groups = make(map[string]float64)
	}

	ts := time.Now().Unix()
	now := ts
	session.Events = append(session.Events, event)
	session.LastUpdate = now
	switch e := event.(type) {
	case Event:
		if e.BaseItem != nil && e.Id > 0 {
			session.ItemEvents.Add(e.Id, DecayEvent{
				TimeStamp: now,
				Value:     200,
			})
			if e.BaseItem.Category == "Gaming" {
				session.Groups["gamer"] += 5
			} else if e.BaseItem.Category3 == "TV" {
				session.Groups["tv"] += 5
			} else if e.BaseItem.Brand == "Apple" {
				session.Groups["apple"] += 3
			}
		} else {
			log.Printf("Event without item %+v", event)
		}
		break
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
		break
	case ImpressionEvent:
		for _, impression := range e.Items {
			session.ItemEvents.Add(impression.Id, DecayEvent{
				TimeStamp: now,
				Value:     10 + (0.02 * float64(max(impression.Position, 300))),
			})
			session.VisitedSkus = append(session.VisitedSkus, impression.Id)
		}
		break
	case CartEvent:
		session.ItemEvents.Add(e.Id, DecayEvent{
			TimeStamp: now,
			Value:     700,
		})
		break
	case ActionEvent:
		if e.BaseItem != nil && e.Id > 0 {
			session.ItemEvents.Add(e.Id, DecayEvent{
				TimeStamp: now,
				Value:     80,
			})
		}
		break
	case PurchaseEvent:
		for _, purchase := range e.Items {
			session.ItemEvents.Add(purchase.Id, DecayEvent{
				TimeStamp: now,
				Value:     800 * float64(purchase.Quantity),
			})
		}
		break
	case SuggestEvent:
		break
	default:
		log.Printf("Unknown event type %T", event)
	}
	return session.Groups
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

	instance := &PersistentMemoryTrackingHandler{
		path:             "data",
		mu:               sync.RWMutex{},
		changes:          0,
		updatesToKeep:    0,
		trackingHandler:  nil,
		ViewedTogether:   make(map[uint]ProductRelation),
		AlsoBought:       make(map[uint]ProductRelation),
		DataSet:          make([]DataSetEvent, 0),
		EmptyResults:     make([]SearchEvent, 0),
		QueryEvents:      make(map[string]QueryMatcher),
		ItemPopularity:   make(sorting.SortOverride),
		Queries:          make(map[string]uint),
		Sessions:         make(map[int64]*SessionData),
		FieldPopularity:  make(sorting.SortOverride),
		ItemEvents:       map[uint][]DecayEvent{},
		FieldEvents:      map[uint][]DecayEvent{},
		FieldValueEvents: make(map[uint]map[string]*DecayPopularity),
		Funnels:          make([]Funnel, 0),
		SortedQueries:    make([]QueryResult, 0),
		FieldValueScores: make(map[uint][]FacetValueResult),
		PersonalizationGroups: map[string]PersonalizationGroup{
			"gamer": {
				Id:          "gamer",
				Name:        "Gamer",
				ItemEvents:  make(map[uint][]DecayEvent),
				FieldEvents: make(map[uint][]DecayEvent),
			},
			"tv": {
				Id:          "tv",
				Name:        "TV",
				ItemEvents:  make(map[uint][]DecayEvent),
				FieldEvents: make(map[uint][]DecayEvent),
			},
			"apple": {
				Id:          "apple",
				Name:        "Apple",
				ItemEvents:  make(map[uint][]DecayEvent),
				FieldEvents: make(map[uint][]DecayEvent),
			},
		},
		//UpdatedItems:    make([]interface{}, 0),
	}

	err := load(path, instance)

	if err != nil {
		log.Printf("Error loading tracking data: %s", err)
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

func load(path string, result *PersistentMemoryTrackingHandler) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	err = json.NewDecoder(file).Decode(result)
	// tmp since the fields does not exist in the json
	if result.ViewedTogether == nil {
		result.ViewedTogether = make(map[uint]ProductRelation)
	}
	if result.AlsoBought == nil {
		result.AlsoBought = make(map[uint]ProductRelation)
	}
	return err
}

func (s *PersistentMemoryTrackingHandler) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Println("Clearing tracking data??")
	//s.changes++
	//s.Sessions = make(map[int64]*SessionData)
	//s.ItemEvents = map[uint][]DecayEvent{}
	//s.FieldEvents = map[uint][]DecayEvent{}
	//s.EmptyResults = make([]SearchEvent, 0)
}

func (s *PersistentMemoryTrackingHandler) GetSession(sessionId int64) *SessionData {
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

type SessionOverview struct {
	*SessionContent
	Id         string `json:"id"`
	Created    int64  `json:"ts"`
	LastUpdate int64  `json:"last_update"`
	LastSync   int64  `json:"last_sync"`
	Events     int    `json:"event_count"`
}

func (s *PersistentMemoryTrackingHandler) GetSessions() []SessionOverview {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sessions := make([]SessionOverview, len(s.Sessions))
	i := 0
	for id, session := range s.Sessions {
		if len(session.Events) > 1 {
			session.Id = id
			log.Printf("Session %d with %d events", id, len(session.Events))
			sessions[i] = SessionOverview{
				SessionContent: session.SessionContent,
				Id:             fmt.Sprintf("%d", id),
				Created:        session.Created,
				LastUpdate:     session.LastUpdate,
				LastSync:       session.LastSync,
				Events:         len(session.Events),
			}
			i++
		}
	}
	return sessions[:i]
}

func (s *PersistentMemoryTrackingHandler) GetFieldPopularity() sorting.SortOverride {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.FieldPopularity
}

func (s *PersistentMemoryTrackingHandler) GetDataSet() []DataSetEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.DataSet
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

	//events := make([]interface{}, 0)
	s.updateSession(event, event.SessionId, nil)
	// s.Sessions[event.SessionId] = &SessionData{
	// 	SessionContent: &event.SessionContent,
	// 	Created:        time.Now().Unix(),
	// 	LastUpdate:     time.Now().Unix(),
	// 	Events:         events,
	// 	VisitedSkus:    make([]string, 0),
	// 	Id:             event.SessionId,
	// 	ItemEvents:     make(map[uint][]DecayEvent),
	// 	FieldEvents:    make(map[uint][]DecayEvent),
	// }
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

func (s *PersistentMemoryTrackingHandler) handleLinkedProducts(session *SessionData, event interface{}) {
	if session == nil {
		return
	}

	switch e := event.(type) {
	case Event:
		if e.BaseItem != nil && e.Id > 0 {
			for _, viewed := range session.VisitedSkus {
				if viewed == e.Id {
					continue
				}
				viewedRelation, ok := s.ViewedTogether[viewed]
				if !ok {
					viewedRelation = ProductRelation{
						ItemId: viewed,
						Other:  make(map[uint]DecayList),
					}
					list := make(DecayList, 0)
					list.Add(e.Id, DecayEvent{
						TimeStamp: time.Now().Unix(),
						Value:     20,
					})

					viewedRelation.Other[e.Id] = list
					s.ViewedTogether[viewed] = viewedRelation
				}
			}
		}
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

func (s *PersistentMemoryTrackingHandler) HandleDataSetEvent(event DataSetEvent, r *http.Request) {
	// log.Printf("DataSet event SessionId: %d, Query: %d, Positive: %s, Negative: %s", event.SessionId, event.Query, event.Positive, event.Negative)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changes++
	go opsProcessed.Inc()

	s.DataSet = append(s.DataSet, event)
}

func normalizeQuery(query string) string {
	query = strings.ToLower(query)
	query = strings.TrimSpace(query)
	return query
}

func (s *PersistentMemoryTrackingHandler) UpdateSessionFromRequest(sessionId int64, r *http.Request) {
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

				for _, value := range filter.Value {
					queryEvents.AddKeyFilterEvent(filter.Id, value)
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

				for _, value := range filter.Value {
					addFieldValueEvent(value)
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

func (s *PersistentMemoryTrackingHandler) updateSession(event interface{}, sessionId int64, r *http.Request) *SessionData {

	session, ok := s.Sessions[sessionId]
	now := time.Now().Unix()
	log.Printf("handling session event %T, session found %v, id: %d", event, ok, sessionId)
	if !ok {
		sessions_total.Inc()
		session = &SessionData{
			SessionContent: GetSessionContentFromRequest(r),
			Created:        now,
			LastUpdate:     now,
			LastSync:       0,
			Id:             sessionId,
			VisitedSkus:    make([]uint, 0),
			Events:         make([]interface{}, 0),
			ItemEvents:     make(map[uint][]DecayEvent),
			FieldEvents:    make(map[uint][]DecayEvent),
		}
		s.Sessions[sessionId] = session
	} else {
		session.LastUpdate = now
		if r != nil {
			session.SessionContent = GetSessionContentFromRequest(r)
		}
	}

	user_groups := session.HandleEvent(event)
	for group, value := range user_groups {
		if group != "" && value > 0 {
			if mainGroup, ok := s.PersonalizationGroups[group]; ok {
				mainGroup.HandleEvent(event)
			}
		}
	}
	return s.Sessions[sessionId]
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
