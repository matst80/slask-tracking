package view

import (
	"encoding/json"
	"log"
	"math"
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
	HandleEvent(event Event)
	HandleSearchEvent(event SearchEventData)
	HandleCartEvent(event CartEvent)
	HandleImpressionEvent(event ImpressionEvent)
	HandleActionEvent(event ActionEvent)
	HandleSuggestEvent(event SuggestEvent)
	GetSession(sessionId int) *SessionData
}

// type UpdateHandler interface {
// 	HandleUpdate(update []interface{})
// }

// type PriceUpdateHandler interface {
// 	HandlePriceUpdate(update []index.DataItem)
// }

type DecayEvent struct {
	TimeStamp int64   `json:"ts"`
	Value     float64 `json:"value"`
}

const (
	decayRate = 0.9999992
	maxAge    = 60 * 60 * 24 * 14
)

func (d *DecayEvent) CalculateValue(now int64) float64 {

	// Calculate the time difference between the event timestamp and the current time.
	timeElapsed := now - d.TimeStamp
	if timeElapsed < 0 {
		// If the timestamp is in the future, return the original value.
		return d.Value
	}
	if timeElapsed > maxAge {
		return 0
	}

	// Apply exponential decay formula.
	decayedValue := d.Value * math.Pow(decayRate, float64(timeElapsed))

	return decayedValue
}

func (d *DecayEvent) Decay(now int64) float64 {
	v := d.CalculateValue(now)
	//if v < 0.1 {
	//	d.TimeStamp = now
	//	d.Value = v
	//}
	return v
}

type DecayArray = []DecayEvent

type DecayPopularity struct {
	Events DecayArray `json:"-"`
	Value  float64    `json:"value"`
}

func (d *DecayPopularity) Add(value DecayEvent) {
	if d.Events == nil {
		d.Events = make([]DecayEvent, 0)
	}
	d.Events = append(d.Events, value)
}

func (d *DecayPopularity) Decay(now int64) float64 {

	var popularity float64

	for _, event := range d.Events {
		popularity += event.Decay(now)
	}
	d.Value = popularity
	return popularity
}

func (d *DecayPopularity) RemoveOlderThan(when int64) {
	end := len(d.Events)

	for i, e := range d.Events {
		if e.TimeStamp >= when {
			end = i
		}
	}
	d.Events = d.Events[:end]
}

type DecayList map[uint]DecayArray

func (d *DecayList) Add(key uint, value DecayEvent) {
	f, ok := (*d)[key]
	if !ok {
		(*d)[key] = []DecayEvent{
			value,
		}
	} else {
		f = append(f, value)
	}
}

func (d *DecayList) Decay(now int64) index.SortOverride {
	result := index.SortOverride{}
	var popularity float64
	var event DecayEvent
	toDelete := make([]uint, 0, len(*d))
	for itemId, events := range *d {
		popularity = 0
		for _, event = range events {
			popularity += event.Decay(now)
		}
		if popularity > 0.3 {
			result[itemId] = popularity
		} else {
			toDelete = append(toDelete, itemId)
		}

	}
	for _, id := range toDelete {
		delete(*d, id)
	}
	return result
}

type QueryKeyData struct {
	FieldPopularity *DecayPopularity            `json:"popularity"`
	ValuePopularity map[string]*DecayPopularity `json:"values"`
}

type QueryMatcher struct {
	Popularity *DecayPopularity      `json:"popularity"`
	Query      string                `json:"query"`
	KeyFields  map[uint]QueryKeyData `json:"keyFacets"`
}

func (q *QueryMatcher) AddKeyFilterEvent(key uint, value string) {

	popularity, ok := q.KeyFields[key]
	if !ok {
		popularity = QueryKeyData{
			FieldPopularity: &DecayPopularity{},
			ValuePopularity: make(map[string]*DecayPopularity),
		}
		q.KeyFields[key] = popularity
	}
	popularity.FieldPopularity.Add(DecayEvent{
		TimeStamp: time.Now().Unix(),
		Value:     100,
	})
	if value != "" {
		valuePopularity, ok := popularity.ValuePopularity[value]
		if !ok {
			valuePopularity = &DecayPopularity{}
			popularity.ValuePopularity[value] = valuePopularity
		}
		valuePopularity.Add(DecayEvent{
			TimeStamp: time.Now().Unix(),
			Value:     100,
		})
	}

}

type PersistentMemoryTrackingHandler struct {
	path            string
	mu              sync.RWMutex
	changes         uint
	updatesToKeep   int
	trackingHandler PopularityListener
	ItemPopularity  index.SortOverride      `json:"item_popularity"`
	Queries         map[string]uint         `json:"queries"`
	QueryEvents     map[string]QueryMatcher `json:"suggestions"`
	Sessions        map[int]*SessionData    `json:"sessions"`
	FieldPopularity index.SortOverride      `json:"field_popularity"`
	ItemEvents      DecayList               `json:"item_events"`
	FieldEvents     DecayList               `json:"field_events"`
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
	start := max(0, len(session.Events)-eventLimit)
	session.Events = append(session.Events[start:], event)
	ts := time.Now().Unix()
	now := ts / 60

	session.LastUpdate = now
	switch e := event.(type) {
	case Event:
		session.ItemEvents.Add(e.Item, DecayEvent{
			TimeStamp: now,
			Value:     509,
		})

	case SearchEventData:

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
	case ImpressionEvent:
		for _, impression := range e.Items {
			session.ItemEvents.Add(impression.Id, DecayEvent{
				TimeStamp: now,
				Value:     0.5 * float64(impression.Position),
			})
		}

	case CartEvent:
		session.ItemEvents.Add(e.Item, DecayEvent{
			TimeStamp: now,
			Value:     700,
		})

	case ActionEvent:
		session.ItemEvents.Add(e.Item, DecayEvent{
			TimeStamp: now,
			Value:     80,
		})

	case PurchaseEvent:
		for _, purchase := range e.Items {
			session.ItemEvents.Add(purchase.Id, DecayEvent{
				TimeStamp: now,
				Value:     800 * float64(purchase.Quantity),
			})

		}
	}
}

func (session *SessionData) DecayEvents(trk PopularityListener) {
	ts := time.Now().Unix()
	now := ts / 60

	session.LastSync = ts
	sf := len(session.FieldEvents)
	if sf > 0 {
		//log.Printf("Decaying field events %d", sf)
		session.FieldPopularity = session.FieldEvents.Decay(now)
		//log.Printf("Session field popularity %d", len(session.FieldPopularity))
		if len(session.FieldPopularity) > 0 {
			if err := trk.SessionFieldPopularityChanged(session.Id, &session.FieldPopularity); err != nil {
				log.Println(err)
			}
		}
	}

	si := len(session.ItemEvents)
	if si > 0 {
		//log.Printf("Decaying item events %d", si)
		session.ItemPopularity = session.ItemEvents.Decay(now)
		if len(session.ItemPopularity) > 0 {
			if err := trk.SessionPopularityChanged(session.Id, &session.ItemPopularity); err != nil {
				log.Println(err)
			}
		}
	}
}

var (
	opsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slasktracking_processed_tracking_events_total",
		Help: "The total number of processed tracking events",
	})
	updatedProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slasktracking_processed_update_events_total",
		Help: "The total number of processed update events",
	})
	updatedItemsProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "slasktracking_processed_item_updates_total",
		Help: "The total number of processed items updates",
	})
)

func (s *PersistentMemoryTrackingHandler) ConnectPopularityListener(handler PopularityListener) {
	s.trackingHandler = handler
}

func MakeMemoryTrackingHandler(path string, itemsToKeep int) *PersistentMemoryTrackingHandler {
	instance, err := load(path)
	if err != nil {
		instance = &PersistentMemoryTrackingHandler{
			path:            "data",
			mu:              sync.RWMutex{},
			changes:         0,
			updatesToKeep:   0,
			trackingHandler: nil,
			QueryEvents:     make(map[string]QueryMatcher),
			ItemPopularity:  make(index.SortOverride),
			Queries:         make(map[string]uint),
			Sessions:        make(map[int]*SessionData),
			FieldPopularity: make(index.SortOverride),
			ItemEvents:      map[uint][]DecayEvent{},
			FieldEvents:     map[uint][]DecayEvent{},
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
	// if instance.UpdatedItems == nil {
	// 	instance.UpdatedItems = make([]interface{}, 0)
	// }
	return instance
}

func (s *PersistentMemoryTrackingHandler) Save() {
	s.save()
}

func (s *PersistentMemoryTrackingHandler) DecayEvents() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	l := len(s.ItemEvents) + len(s.FieldEvents)
	if l == 0 {
		return
	}

	s.ItemPopularity = s.ItemEvents.Decay(now)
	s.FieldPopularity = s.FieldEvents.Decay(now)

	log.Printf("Decayed events %d", l)
}

func (s *PersistentMemoryTrackingHandler) DecaySuggestions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()
	for _, suggestion := range s.QueryEvents {
		suggestion.Popularity.Decay(now)
		for _, keyField := range suggestion.KeyFields {
			keyField.FieldPopularity.Decay(now)
			for _, v := range keyField.ValuePopularity {
				v.Decay(now)
			}
		}
	}
	log.Printf("Decayed suggestions %d", len(s.QueryEvents))
}

func (s *PersistentMemoryTrackingHandler) cleanSessions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.Sessions) > 50000 {
		log.Println("Cleaning sessions")
		tm := time.Now()
		limit := tm.Unix() - 60*60*24*7
		for key, item := range s.Sessions {
			if limit > item.LastUpdate {
				log.Printf("Deleting session %d", key)
				delete(s.Sessions, key)
			}
		}
	}
}

func (s *PersistentMemoryTrackingHandler) DecaySessionEvents() {
	if s.trackingHandler != nil {
		for id, session := range s.Sessions {
			if session.Id != id {
				session.Id = id
			}
			session.DecayEvents(s.trackingHandler)
		}
	}
}

func (s *PersistentMemoryTrackingHandler) save() error {
	s.DecaySuggestions()
	s.DecayEvents()
	s.DecaySessionEvents()
	s.cleanSessions()
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
	if err == nil && result.QueryEvents == nil {
		result.QueryEvents = make(map[string]QueryMatcher)
	}

	return result, err
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

func (s *PersistentMemoryTrackingHandler) GetItemPopularity() map[uint]float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ItemPopularity
}

func (s *PersistentMemoryTrackingHandler) GetSuggestions(q string) interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if q == "" {
		return s.QueryEvents
	}
	ret := make(map[string]QueryMatcher)
	for key, event := range s.QueryEvents {
		if strings.Contains(key, q) {
			ret[key] = event
		}
	}
	return ret
}

// func (s *PersistentMemoryTrackingHandler) GetUpdatedItems() []interface{} {
// 	s.mu.RLock()
// 	defer s.mu.RUnlock()
// 	return s.UpdatedItems
// }

func (s *PersistentMemoryTrackingHandler) GetQueries() map[string]uint {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Queries
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

// func (s *PersistentMemoryTrackingHandler) HandleUpdate(item []interface{}) {
// 	// log.Printf("Session new session event %d", event.SessionId)
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	s.changes++
// 	updatedProcessed.Inc()
// 	s.UpdatedItems = append(s.UpdatedItems, item...)
// 	updatedItemsProcessed.Add(float64(len(item)))
// 	diff := len(s.UpdatedItems) - s.updatesToKeep
// 	if diff > 0 {
// 		s.UpdatedItems = s.UpdatedItems[len(s.UpdatedItems)-diff:]
// 	}
// }

// func (s *PersistentMemoryTrackingHandler) HandlePriceUpdate(item []index.DataItem) {
// 	for _, item := range item {
// 		log.Printf("Price update %d, url: %s", item.Id, "https://www.elgiganten.se"+item.Url)
// 	}
// }

func (s *PersistentMemoryTrackingHandler) GetFieldPopularity() index.SortOverride {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.FieldPopularity
}

func (s *PersistentMemoryTrackingHandler) HandleSessionEvent(event Session) {
	// log.Printf("Session new session event %d", event.SessionId)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changes++
	opsProcessed.Inc()

	events := make([]interface{}, 0, 200)
	s.Sessions[event.SessionId] = &SessionData{
		SessionContent: &event.SessionContent,
		Created:        time.Now().Unix(),
		LastUpdate:     time.Now().Unix(),
		Events:         events,
		ItemEvents:     make(map[uint][]DecayEvent),
		FieldEvents:    make(map[uint][]DecayEvent),
	}
}

//func (s *PersistentMemoryTrackingHandler) appendItemEvent(itemId uint, value float64) {
//	if s.ItemEvents == nil {
//		s.ItemEvents = make(map[uint][]DecayEvent)
//	}
//	s.ItemEvents.Add(itemId, DecayEvent{
//		TimeStamp: time.Now().Unix(),
//		Value:     value,
//	})
//}
//
//func (s *PersistentMemoryTrackingHandler) appendFieldEvent(fieldId uint, value float64) {
//	if s.FieldEvents == nil {
//		s.FieldEvents = make(map[uint][]DecayEvent)
//	}
//	s.FieldEvents.Add(fieldId, DecayEvent{
//		TimeStamp: time.Now().Unix(),
//		Value:     value,
//	})
//
//}

func (s *PersistentMemoryTrackingHandler) HandleEvent(event Event) {
	// log.Printf("Event SessionId: %d, ItemId: %d, Position: %f", event.SessionId, event.Item, event.Position)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ItemEvents.Add(event.Item, DecayEvent{
		TimeStamp: time.Now().Unix(),
		Value:     40,
	})

	s.updateSession(event, event.SessionId)

	s.changes++
	go opsProcessed.Inc()
}

func (s *PersistentMemoryTrackingHandler) HandleCartEvent(event CartEvent) {
	// log.Printf("Cart event SessionId: %d, ItemId: %d, Quantity: %d", event.SessionId, event.Item, event.Quantity)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ItemEvents.Add(event.Item, DecayEvent{
		TimeStamp: time.Now().Unix(),
		Value:     140,
	})
	s.changes++
	go opsProcessed.Inc()
	s.updateSession(event, event.SessionId)
}

func (s *PersistentMemoryTrackingHandler) HandleSearchEvent(event SearchEventData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changes++
	go opsProcessed.Inc()
	ts := time.Now().Unix()
	if event.Query != "" {
		s.Queries[event.Query] += 2

		if event.Query != "" {
			queryEvents, ok := s.QueryEvents[event.Query]
			if !ok {
				queryEvents = QueryMatcher{
					Query:      event.Query,
					Popularity: &DecayPopularity{},
					KeyFields:  make(map[uint]QueryKeyData),
				}
				s.QueryEvents[event.Query] = queryEvents
			}
			queryEvents.Popularity.Add(DecayEvent{
				TimeStamp: ts,
				Value:     100,
			})
			//queryEvents.Popularity.Decay(ts)
			for _, filter := range event.Filters.StringFilter {
				switch filter.Value.(type) {
				case string:
					queryEvents.AddKeyFilterEvent(filter.Id, filter.Value.(string))
				case []string:
					for _, value := range filter.Value.([]string) {
						queryEvents.AddKeyFilterEvent(filter.Id, value)
					}
				default:
					log.Printf("Unknown type %T for filter %d", filter.Value, filter.Id)
				}

			}
		}

	}

	for _, filter := range event.Filters.StringFilter {
		s.FieldEvents.Add(filter.Id, DecayEvent{
			TimeStamp: ts,
			Value:     6,
		})
	}
	for _, filter := range event.Filters.RangeFilter {
		s.FieldEvents.Add(filter.Id, DecayEvent{
			TimeStamp: ts,
			Value:     3,
		})
	}
	s.updateSession(event, event.SessionId)

}

func (s *PersistentMemoryTrackingHandler) updateSession(event interface{}, sessionId int) {

	session, ok := s.Sessions[sessionId]
	if !ok {
		session = &SessionData{
			SessionContent: &SessionContent{},
			Created:        time.Now().Unix(),
			LastUpdate:     time.Now().Unix(),
			LastSync:       0,
			Id:             sessionId,
			Events:         make([]interface{}, 0),
			ItemEvents:     make(map[uint][]DecayEvent),
			FieldEvents:    make(map[uint][]DecayEvent),
		}
		s.Sessions[sessionId] = session
	}

	session.HandleEvent(event)

}

func (s *PersistentMemoryTrackingHandler) HandleImpressionEvent(event ImpressionEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	go opsProcessed.Inc()
	for _, impression := range event.Items {
		s.ItemPopularity[impression.Id] += 0.01 + float64(impression.Position)/1000
	}
	s.updateSession(event, event.SessionId)
	s.changes++

}

func (s *PersistentMemoryTrackingHandler) HandleActionEvent(event ActionEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	go opsProcessed.Inc()
	s.updateSession(event, event.SessionId)
	s.changes++
}

func (s *PersistentMemoryTrackingHandler) HandleSuggestEvent(event SuggestEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	go opsProcessed.Inc()
	s.updateSession(event, event.SessionId)
	s.Queries[event.Value] += 1
	log.Printf("Suggest %s", event.Value)
	s.changes++
}
