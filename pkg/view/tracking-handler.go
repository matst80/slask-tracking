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

	"github.com/matst80/slask-finder/pkg/types"
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
	FacetId uint32             `json:"id"`
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
	KeyFields map[uint32]QueryKeyData `json:"keyFacets"`
}

func (q *QueryMatcher) AddKeyFilterEvent(key uint32, value string) {
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
	ItemId uint32             `json:"item_id"`
	Other  map[uint32]DecayList `json:"other"`
}

type PersistentMemoryTrackingHandler struct {
	path                  string
	mu                    sync.RWMutex
	changes               uint
	updatesToKeep         int
	trackingHandler       PopularityListener
	followers             []TrackingHandler
	boltStorage           *BoltStorage
	aofLogger             *AOFLogger
	eventChan             chan func()
	decayers              []Decayer
	Config                TrackingConfig                       `json:"tracking_config"`
	ViewedTogether        map[uint32]ProductRelation             `json:"viewed_together"`
	AlsoBought            map[uint32]ProductRelation             `json:"also_bought"`
	DataSet               []DataSetEvent                       `json:"dataset"`
	FieldValueScores      map[uint32][]FacetValueResult          `json:"field_value_scores"`
	ItemPopularity        types.SortOverride                 `json:"item_popularity"`
	Queries               map[string]uint                      `json:"queries"`
	QueryEvents           map[string]QueryMatcher              `json:"suggestions"`
	Sessions              map[int64]*SessionData               `json:"sessions"`
	FieldPopularity       types.SortOverride                 `json:"field_popularity"`
	ItemEvents            DecayList                            `json:"item_events"`
	FieldEvents           DecayList                            `json:"field_events"`
	SortedQueries         []QueryResult                        `json:"sorted_queries"`
	FieldValueEvents      map[uint32]map[string]*DecayPopularity `json:"field_value_events"`
	Funnels               []Funnel                             `json:"funnel_storage"`
	EmptyResults          []SearchEvent                        `json:"empty_results_v2"`
	PersonalizationGroups map[string]PersonalizationGroup      `json:"personalization_groups"`
	//UpdatedItems    []interface{}        `json:"updated_items"`
}

type SessionData struct {
	*SessionContent
	VisitedSkus []uint32                 `json:"visited_skus"`
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

func (session *SessionData) HandleEvent(event interface{}, cfg *TrackingConfig) map[string]float64 {
	if session.ItemEvents == nil {
		session.ItemEvents = make(map[uint32][]DecayEvent)
	}
	if session.FieldEvents == nil {
		session.FieldEvents = make(map[uint32][]DecayEvent)
	}
	if session.VisitedSkus == nil {
		session.VisitedSkus = make([]uint32, 0)
	}
	if session.Events == nil {
		log.Printf("make new event-list, %d", session.Id)
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
			if cfg != nil {
				for _, rule := range cfg.PersonaRules {
					matched := true
					for field, value := range rule.Conditions {
						switch field {
						case "category":
							if e.BaseItem.Category != value {
								matched = false
							}
						case "category2":
							if e.BaseItem.Category2 != value {
								matched = false
							}
						case "category3":
							if e.BaseItem.Category3 != value {
								matched = false
							}
						case "category4":
							if e.BaseItem.Category4 != value {
								matched = false
							}
						case "category5":
							if e.BaseItem.Category5 != value {
								matched = false
							}
						case "brand":
							if e.BaseItem.Brand != value {
								matched = false
							}
						case "name":
							if e.BaseItem.Name != value {
								matched = false
							}
						default:
							matched = false
						}
					}
					if matched {
						session.Groups[rule.Persona] += rule.Points
					}
				}
			}
		} else {
			log.Printf("Event without item %+v", event)
		}

	case SearchEvent:
		for _, filter := range e.Filters.StringFilter {
			session.FieldEvents.Add(uint32(filter.Id), DecayEvent{
				TimeStamp: now,
				Value:     150,
			})
		}
		for _, filter := range e.Filters.RangeFilter {
			session.FieldEvents.Add(uint32(filter.Id), DecayEvent{
				TimeStamp: now,
				Value:     100,
			})
		}

	case ImpressionEvent:
		for _, impression := range e.Items {
			session.ItemEvents.Add(impression.Id, DecayEvent{
				TimeStamp: now,
				Value:     10 + (0.02 * float64(max(impression.Position, 300))),
			})
			session.VisitedSkus = append(session.VisitedSkus, impression.Id)
		}

	case CartEvent:
		session.ItemEvents.Add(e.Id, DecayEvent{
			TimeStamp: now,
			Value:     700,
		})

	case ActionEvent:
		if e.BaseItem != nil && e.Id > 0 {
			session.ItemEvents.Add(e.Id, DecayEvent{
				TimeStamp: now,
				Value:     80,
			})
		}

	case PurchaseEvent:
		for _, purchase := range e.Items {
			session.ItemEvents.Add(purchase.Id, DecayEvent{
				TimeStamp: now,
				Value:     800 * float64(purchase.Quantity),
			})
		}

	case SuggestEvent:

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

func (s *PersistentMemoryTrackingHandler) AttachFollower(handler TrackingHandler) {
	if handler == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.followers = append(s.followers, handler)
}

func (s *PersistentMemoryTrackingHandler) dispatchFollowers(dispatch func(TrackingHandler)) {
	if dispatch == nil {
		return
	}
	s.mu.RLock()
	followers := make([]TrackingHandler, len(s.followers))
	copy(followers, s.followers)
	s.mu.RUnlock()
	for _, follower := range followers {
		follower := follower
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("tracking follower panic: %v", r)
				}
			}()
			dispatch(follower)
		}()
	}
}

func MakeMemoryTrackingHandler(path string, itemsToKeep int) *PersistentMemoryTrackingHandler {
	boltStorage, err := NewBoltStorage(path + ".db")
	if err != nil {
		log.Fatalf("Failed to open BoltDB storage: %v", err)
	}

	aofLogger, err := NewAOFLogger(path + ".aof")
	if err != nil {
		log.Fatalf("Failed to initialize AOF logger: %v", err)
	}

	instance := &PersistentMemoryTrackingHandler{
		path:             path,
		mu:               sync.RWMutex{},
		changes:          0,
		updatesToKeep:    itemsToKeep,
		trackingHandler:  nil,
		followers:        make([]TrackingHandler, 0),
		boltStorage:      boltStorage,
		aofLogger:        aofLogger,
		eventChan:        make(chan func(), 10000),
		ViewedTogether:   make(map[uint32]ProductRelation),
		AlsoBought:       make(map[uint32]ProductRelation),
		DataSet:          make([]DataSetEvent, 0),
		EmptyResults:     make([]SearchEvent, 0),
		QueryEvents:      make(map[string]QueryMatcher),
		ItemPopularity:   make(types.SortOverride),
		Queries:          make(map[string]uint),
		Sessions:         make(map[int64]*SessionData),
		FieldPopularity:  make(types.SortOverride),
		ItemEvents:       map[uint32][]DecayEvent{},
		FieldEvents:      map[uint32][]DecayEvent{},
		FieldValueEvents: make(map[uint32]map[string]*DecayPopularity),
		Funnels:          make([]Funnel, 0),
		SortedQueries:    make([]QueryResult, 0),
		FieldValueScores: make(map[uint32][]FacetValueResult),
		PersonalizationGroups: map[string]PersonalizationGroup{
			"gamer": {
				Id:          "gamer",
				Name:        "Gamer",
				ItemEvents:  make(map[uint32][]DecayEvent),
				FieldEvents: make(map[uint32][]DecayEvent),
			},
			"tv": {
				Id:          "tv",
				Name:        "TV",
				ItemEvents:  make(map[uint32][]DecayEvent),
				FieldEvents: make(map[uint32][]DecayEvent),
			},
			"apple": {
				Id:          "apple",
				Name:        "Apple",
				ItemEvents:  make(map[uint32][]DecayEvent),
				FieldEvents: make(map[uint32][]DecayEvent),
			},
		},
	}

	instance.decayers = []Decayer{
		&GlobalEventsDecayer{handler: instance},
		&SuggestionDecayer{handler: instance},
		&SessionDecayer{handler: instance},
		&GroupDecayer{handler: instance},
		&FacetValuesDecayer{handler: instance},
	}

	// Start single-threaded event loop
	go func() {
		for f := range instance.eventChan {
			f()
		}
	}()

	// Try loading metadata from BoltDB.
	err = instance.loadMetaFromBolt()
	if err != nil {
		// Fallback to load JSON and migrate.
		log.Printf("Metadata not in BoltDB. Attempting to migrate from JSON checkpoint %s: %v", path, err)
		instance.Config = DefaultConfig()
		if errLoad := loadOldJSON(path, instance); errLoad == nil {
			log.Printf("Successfully loaded legacy JSON. Migrating to BoltDB...")
			instance.saveMetaToBolt()
			for _, session := range instance.Sessions {
				boltStorage.SaveSession(session)
			}
		} else {
			log.Printf("No legacy JSON checkpoint found or failed to load: %v. Starting fresh.", errLoad)
			instance.saveMetaToBolt()
		}
	} else {
		// Load sessions cache from BoltDB.
		if sess, errSess := boltStorage.GetAllSessions(); errSess == nil {
			instance.Sessions = sess
		} else {
			log.Printf("Error loading sessions from BoltDB: %v", errSess)
		}
	}

	// Replay AOF log to apply any uncheckpointed events.
	if errReplay := ReplayAOF(path+".aof", instance); errReplay != nil {
		log.Printf("Error replaying AOF file: %v", errReplay)
	}

	go func() {
		for range time.Tick(time.Minute) {
			if instance.changes > 0 {
				instance.Save()
			}
		}
	}()

	return instance
}

func (s *PersistentMemoryTrackingHandler) Save() {
	done := make(chan struct{})
	s.eventChan <- func() {
		s.save()
		close(done)
	}
	<-done
}

func (s *PersistentMemoryTrackingHandler) save() error {
	now := time.Now().Unix()

	for _, decayer := range s.decayers {
		if err := decayer.Decay(now); err != nil {
			log.Printf("Decay error: %v", err)
		}
	}

	s.cleanSessions()

	defer runtime.GC()
	if s.changes == 0 {
		return nil
	}
	if s.trackingHandler != nil {
		go s.trackingHandler.PopularityChanged(&s.ItemPopularity)
		go s.trackingHandler.FieldPopularityChanged(&s.FieldPopularity)
	}

	log.Println("Saving metadata checkpoint to BoltDB and truncating AOF")

	s.changes = 0
	err := s.saveMetaToBolt()
	if err != nil {
		log.Printf("Failed to save metadata to BoltDB: %v", err)
	}

	if errAof := s.aofLogger.Truncate(); errAof != nil {
		log.Printf("Failed to truncate AOF file: %v", errAof)
	}

	return err
}

func (s *PersistentMemoryTrackingHandler) loadMetaFromBolt() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.boltStorage.LoadMeta("viewed_together", &s.ViewedTogether)
	if err != nil {
		return err
	}
	s.boltStorage.LoadMeta("also_bought", &s.AlsoBought)
	s.boltStorage.LoadMeta("dataset", &s.DataSet)
	s.boltStorage.LoadMeta("field_value_scores", &s.FieldValueScores)
	s.boltStorage.LoadMeta("item_popularity", &s.ItemPopularity)
	s.boltStorage.LoadMeta("queries", &s.Queries)
	s.boltStorage.LoadMeta("suggestions", &s.QueryEvents)
	s.boltStorage.LoadMeta("field_popularity", &s.FieldPopularity)
	s.boltStorage.LoadMeta("item_events", &s.ItemEvents)
	s.boltStorage.LoadMeta("field_events", &s.FieldEvents)
	s.boltStorage.LoadMeta("sorted_queries", &s.SortedQueries)
	s.boltStorage.LoadMeta("field_value_events", &s.FieldValueEvents)
	s.boltStorage.LoadMeta("funnel_storage", &s.Funnels)
	s.boltStorage.LoadMeta("empty_results_v2", &s.EmptyResults)
	s.boltStorage.LoadMeta("personalization_groups", &s.PersonalizationGroups)

	if errCfg := s.boltStorage.LoadMeta("tracking_config", &s.Config); errCfg != nil {
		s.Config = DefaultConfig()
	}
	return nil
}

func (s *PersistentMemoryTrackingHandler) saveMetaToBolt() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.boltStorage.SaveMeta("viewed_together", s.ViewedTogether)
	s.boltStorage.SaveMeta("also_bought", s.AlsoBought)
	s.boltStorage.SaveMeta("dataset", s.DataSet)
	s.boltStorage.SaveMeta("field_value_scores", s.FieldValueScores)
	s.boltStorage.SaveMeta("item_popularity", s.ItemPopularity)
	s.boltStorage.SaveMeta("queries", s.Queries)
	s.boltStorage.SaveMeta("suggestions", s.QueryEvents)
	s.boltStorage.SaveMeta("field_popularity", s.FieldPopularity)
	s.boltStorage.SaveMeta("item_events", s.ItemEvents)
	s.boltStorage.SaveMeta("field_events", s.FieldEvents)
	s.boltStorage.SaveMeta("sorted_queries", s.SortedQueries)
	s.boltStorage.SaveMeta("field_value_events", s.FieldValueEvents)
	s.boltStorage.SaveMeta("funnel_storage", s.Funnels)
	s.boltStorage.SaveMeta("empty_results_v2", s.EmptyResults)
	s.boltStorage.SaveMeta("personalization_groups", s.PersonalizationGroups)
	s.boltStorage.SaveMeta("tracking_config", s.Config)
	return nil
}

func loadOldJSON(path string, result *PersistentMemoryTrackingHandler) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	err = json.NewDecoder(file).Decode(result)
	if result.ViewedTogether == nil {
		result.ViewedTogether = make(map[uint32]ProductRelation)
	}
	if result.AlsoBought == nil {
		result.AlsoBought = make(map[uint32]ProductRelation)
	}
	return err
}

func (s *PersistentMemoryTrackingHandler) Clear() {
	s.eventChan <- func() {
		log.Println("Clearing tracking data")
		s.changes++
		s.Sessions = make(map[int64]*SessionData)
		s.ItemEvents = map[uint32][]DecayEvent{}
		s.FieldEvents = map[uint32][]DecayEvent{}
		s.EmptyResults = make([]SearchEvent, 0)
		s.saveMetaToBolt()
	}
}

func (s *PersistentMemoryTrackingHandler) GetSession(sessionId int64) *SessionData {
	session, err := s.boltStorage.GetSession(sessionId)
	if err == nil && session != nil {
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

func (s *PersistentMemoryTrackingHandler) GetItemPopularity() types.SortOverride {
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

func (s *PersistentMemoryTrackingHandler) GetFieldPopularity() types.SortOverride {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.FieldPopularity
}

func (s *PersistentMemoryTrackingHandler) GetDataSet() []DataSetEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.DataSet
}

func (s *PersistentMemoryTrackingHandler) GetFieldValuePopularity(id uint32) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	values, ok := s.FieldValueScores[id]
	if !ok {
		return nil
	}
	return values
}

func (s *PersistentMemoryTrackingHandler) HandleSessionEvent(event Session) {
	s.dispatchFollowers(func(handler TrackingHandler) {
		handler.HandleSessionEvent(event)
	})
	s.aofLogger.LogEvent(EVENT_SESSION_START, event)
	s.eventChan <- func() {
		s.changes++
		opsProcessed.Inc()
		s.updateSession(event, event.SessionId, nil)
	}
}

func (s *PersistentMemoryTrackingHandler) HandleEvent(event Event, r *http.Request) {
	s.dispatchFollowers(func(handler TrackingHandler) {
		handler.HandleEvent(event, r)
	})
	s.aofLogger.LogEvent(EVENT_ITEM_CLICK, event)
	s.eventChan <- func() {
		s.ItemEvents.Add(event.Id, DecayEvent{
			TimeStamp: time.Now().Unix(),
			Value:     s.Config.CalculateEventValue(EVENT_ITEM_CLICK, event.BaseItem),
		})

		s.handleFunnels(&event)
		s.updateSession(event, event.SessionId, r)

		s.changes++
		opsProcessed.Inc()
	}
}

func (s *PersistentMemoryTrackingHandler) handleFunnels(event TrackingEvent) {
	s.mu.RLock()
	defer s.mu.RUnlock()
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
						Other:  make(map[uint32]DecayList),
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
	s.dispatchFollowers(func(handler TrackingHandler) {
		handler.HandleEnterCheckout(event, r)
	})
	s.aofLogger.LogEvent(CART_ENTER_CHECKOUT, event)
	s.eventChan <- func() {
		for _, item := range event.Items {
			s.ItemEvents.Add(item.Id, DecayEvent{
				TimeStamp: time.Now().Unix(),
				Value:     s.Config.CalculateEventValue(CART_ENTER_CHECKOUT, &item),
			})
		}
		s.changes++
		opsProcessed.Inc()
		s.handleFunnels(&event)
		s.updateSession(event, event.SessionId, r)
	}
}

func (s *PersistentMemoryTrackingHandler) HandleCartEvent(event CartEvent, r *http.Request) {
	s.dispatchFollowers(func(handler TrackingHandler) {
		handler.HandleCartEvent(event, r)
	})
	s.aofLogger.LogEvent(event.Event, event)
	s.eventChan <- func() {
		s.ItemEvents.Add(event.Id, DecayEvent{
			TimeStamp: time.Now().Unix(),
			Value:     s.Config.CalculateEventValue(event.Event, event.BaseItem),
		})
		s.changes++
		opsProcessed.Inc()
		s.handleFunnels(&event)
		s.updateSession(event, event.SessionId, r)
	}
}

func (s *PersistentMemoryTrackingHandler) HandleDataSetEvent(event DataSetEvent, r *http.Request) {
	s.dispatchFollowers(func(handler TrackingHandler) {
		handler.HandleDataSetEvent(event, r)
	})
	s.aofLogger.LogEvent(EVENT_DATA_SET, event)
	s.eventChan <- func() {
		s.changes++
		opsProcessed.Inc()
		s.DataSet = append(s.DataSet, event)
	}
}

func normalizeQuery(query string) string {
	query = strings.ToLower(query)
	query = strings.TrimSpace(query)
	return query
}

func (s *PersistentMemoryTrackingHandler) UpdateSessionFromRequest(sessionId int64, r *http.Request) {
	s.eventChan <- func() {
		session, ok := s.Sessions[sessionId]
		if ok {
			session.Ip = r.RemoteAddr
			s.boltStorage.SaveSession(session)
		}
	}
}

func (s *PersistentMemoryTrackingHandler) HandleSearchEvent(event SearchEvent, r *http.Request) {
	s.dispatchFollowers(func(handler TrackingHandler) {
		handler.HandleSearchEvent(event, r)
	})
	s.aofLogger.LogEvent(EVENT_SEARCH, event)
	s.eventChan <- func() {
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
		s.changes++
		opsProcessed.Inc()
		ts := time.Now().Unix()

		if event.Query != "" && event.Query != "*" {
			normalizedQuery := normalizeQuery(event.Query)
			s.Queries[normalizedQuery] += 1

			if normalizedQuery != "" {
				queryEvents, ok := s.QueryEvents[normalizedQuery]
				if !ok {
					queryEvents = QueryMatcher{
						Popularity: &DecayPopularity{},
						KeyFields:  make(map[uint32]QueryKeyData),
					}
					s.QueryEvents[normalizedQuery] = queryEvents
				}
				queryEvents.Popularity.Add(DecayEvent{
					TimeStamp: ts,
					Value:     s.Config.CalculateEventValue(EVENT_SEARCH, nil),
				})
				for _, filter := range event.Filters.StringFilter {
					for _, value := range filter.Value {
						queryEvents.AddKeyFilterEvent(uint32(filter.Id), value)
					}
				}
			}
		} else {
			for _, filter := range event.Filters.StringFilter {
				s.FieldEvents.Add(uint32(filter.Id), DecayEvent{
					TimeStamp: ts,
					Value:     40.0,
				})
				fieldValues, ok := s.FieldValueEvents[uint32(filter.Id)]
				if !ok {
					fieldValues = make(map[string]*DecayPopularity)
					s.FieldValueEvents[uint32(filter.Id)] = fieldValues
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
			for _, filter := range event.Filters.RangeFilter {
				s.FieldEvents.Add(uint32(filter.Id), DecayEvent{
					TimeStamp: ts,
					Value:     30,
				})
			}
		}

		s.handleFunnels(&event)
		s.updateSession(event, event.SessionId, r)
	}
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
			VisitedSkus:    make([]uint32, 0),
			Events:         make([]interface{}, 0),
			ItemEvents:     make(map[uint32][]DecayEvent),
			FieldEvents:    make(map[uint32][]DecayEvent),
		}
		s.Sessions[sessionId] = session
	} else {
		session.LastUpdate = now
		if r != nil {
			session.SessionContent = GetSessionContentFromRequest(r)
		}
	}

	user_groups := session.HandleEvent(event, &s.Config)
	for group, value := range user_groups {
		if group != "" && value > 0 {
			if mainGroup, ok := s.PersonalizationGroups[group]; ok {
				mainGroup.HandleEvent(event)
			}
		}
	}
	s.boltStorage.SaveSession(session)
	return session
}

func (s *PersistentMemoryTrackingHandler) HandleImpressionEvent(event ImpressionEvent, r *http.Request) {
	s.dispatchFollowers(func(handler TrackingHandler) {
		handler.HandleImpressionEvent(event, r)
	})
	s.aofLogger.LogEvent(EVENT_ITEM_IMPRESS, event)
	s.eventChan <- func() {
		opsProcessed.Inc()
		for _, impression := range event.Items {
			s.ItemEvents.Add(impression.Id, DecayEvent{
				TimeStamp: time.Now().Unix(),
				Value:     s.Config.CalculateEventValue(EVENT_ITEM_IMPRESS, &impression),
			})
		}
		s.updateSession(event, event.SessionId, r)
		s.handleFunnels(&event)
		s.changes++
	}
}

func (s *PersistentMemoryTrackingHandler) HandleActionEvent(event ActionEvent, r *http.Request) {
	s.dispatchFollowers(func(handler TrackingHandler) {
		handler.HandleActionEvent(event, r)
	})
	s.aofLogger.LogEvent(EVENT_ITEM_ACTION, event)
	s.eventChan <- func() {
		opsProcessed.Inc()
		if event.BaseItem != nil && event.Id > 0 {
			s.ItemEvents.Add(event.Id, DecayEvent{
				TimeStamp: time.Now().Unix(),
				Value:     s.Config.CalculateEventValue(EVENT_ITEM_ACTION, event.BaseItem),
			})
		}
		s.updateSession(event, event.SessionId, r)
		s.handleFunnels(&event)
		s.changes++
	}
}

func (s *PersistentMemoryTrackingHandler) HandleSuggestEvent(event SuggestEvent, r *http.Request) {
	s.dispatchFollowers(func(handler TrackingHandler) {
		handler.HandleSuggestEvent(event, r)
	})
	s.aofLogger.LogEvent(EVENT_SUGGEST, event)
	s.eventChan <- func() {
		opsProcessed.Inc()
		s.updateSession(event, event.SessionId, r)
		s.handleFunnels(&event)
		s.Queries[event.Value] += 1
		s.changes++
	}
}

func (s *PersistentMemoryTrackingHandler) UpdateConfig(cfg TrackingConfig) {
	done := make(chan struct{})
	s.eventChan <- func() {
		s.Config = cfg
		s.boltStorage.SaveMeta("tracking_config", cfg)
		s.changes++
		close(done)
	}
	<-done
}
