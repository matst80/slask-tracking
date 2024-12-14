package view

import (
	"encoding/json"
	"log"
	"os"
	"runtime"
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
	GetSession(sessionId int) *SessionData
}

type UpdateHandler interface {
	HandleUpdate(update []interface{})
}

type PriceUpdateHandler interface {
	HandlePriceUpdate(update []index.DataItem)
}

type PersistentMemoryTrackingHandler struct {
	path            string
	mu              sync.RWMutex
	changes         uint
	updatesToKeep   int
	trackingHandler PopularityListener
	ItemPopularity  index.SortOverride   `json:"item_popularity"`
	Queries         map[string]uint      `json:"queries"`
	Sessions        map[int]*SessionData `json:"sessions"`
	FieldPopularity index.SortOverride   `json:"field_popularity"`
	UpdatedItems    []interface{}        `json:"updated_items"`
}

type SessionData struct {
	*SessionContent
	Events        []interface{}          `json:"events"`
	PopularItems  index.SortOverride     `json:"popular_items"`
	PopularFacets map[uint][]interface{} `json:"popular_facets"`
	Created       int64                  `json:"ts"`
	LastUpdate    int64                  `json:"last_update"`
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

func (m *PersistentMemoryTrackingHandler) ConnectPopularityListener(handler PopularityListener) {
	m.trackingHandler = handler
}

func MakeMemoryTrackingHandler(path string, itemsToKeep int) *PersistentMemoryTrackingHandler {
	instance, err := load(path)
	if err != nil {
		instance = &PersistentMemoryTrackingHandler{
			ItemPopularity:  make(index.SortOverride),
			Queries:         make(map[string]uint),
			Sessions:        make(map[int]*SessionData),
			FieldPopularity: make(index.SortOverride),
		}
	}
	go func() {
		for range time.Tick(time.Minute) {
			if instance.changes > 0 {
				instance.save()
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
	if instance.UpdatedItems == nil {
		instance.UpdatedItems = make([]interface{}, 0)
	}
	return instance
}

func (s *PersistentMemoryTrackingHandler) Save() {
	s.save()
}

func (s *PersistentMemoryTrackingHandler) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer runtime.GC()
	if s.changes == 0 {
		return nil
	}
	if s.trackingHandler != nil {
		go s.trackingHandler.PopularityChanged(&s.ItemPopularity)
		go s.trackingHandler.FieldPopularityChanged(&s.FieldPopularity)
	}
	if len(s.Sessions) > 500 {
		log.Println("Cleaning sessions")
		tm := time.Now()
		limit := tm.Unix() - 60*60*24*7
		for key, item := range s.Sessions {
			if len(item.Events) < 10 || limit > item.LastUpdate {
				delete(s.Sessions, key)
			} else {
				item.Events = item.Events[max(0, len(item.Events)-50):]
			}
		}
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
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	err = json.NewEncoder(file).Encode(s)
	return err
}

func (m *PersistentMemoryTrackingHandler) GetItemPopularity() map[uint]float64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ItemPopularity
}

func (m *PersistentMemoryTrackingHandler) GetUpdatedItems() []interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.UpdatedItems
}

func (m *PersistentMemoryTrackingHandler) GetQueries() map[string]uint {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.Queries
}

func (m *PersistentMemoryTrackingHandler) GetSessions() []*SessionData {
	m.mu.RLock()
	defer m.mu.RUnlock()
	sessions := make([]*SessionData, len(m.Sessions))
	i := 0
	for _, session := range m.Sessions {
		if len(session.Events) > 1 {
			sessions[i] = session
			i++
		}
	}
	return sessions[:i]
}

func (m *PersistentMemoryTrackingHandler) HandleUpdate(item []interface{}) {
	// log.Printf("Session new session event %d", event.SessionId)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changes++
	updatedProcessed.Inc()
	m.UpdatedItems = append(m.UpdatedItems, item...)
	updatedItemsProcessed.Add(float64(len(item)))
	diff := len(m.UpdatedItems) - m.updatesToKeep
	if diff > 0 {
		m.UpdatedItems = m.UpdatedItems[len(m.UpdatedItems)-diff:]
	}
}

func (m *PersistentMemoryTrackingHandler) HandlePriceUpdate(item []index.DataItem) {
	for _, item := range item {
		log.Printf("Price update %d, url: %s", item.Id, "https://www.elgiganten.se"+item.Url)
	}
}

func (m *PersistentMemoryTrackingHandler) GetFieldPopularity() index.SortOverride {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.FieldPopularity
}

func (m *PersistentMemoryTrackingHandler) HandleSessionEvent(event Session) {
	// log.Printf("Session new session event %d", event.SessionId)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changes++
	opsProcessed.Inc()

	events := make([]interface{}, 0, 200)
	m.Sessions[event.SessionId] = &SessionData{
		SessionContent: &event.SessionContent,
		Created:        time.Now().Unix(),
		LastUpdate:     time.Now().Unix(),
		Events:         events,
		PopularItems:   make(index.SortOverride),
		PopularFacets:  make(map[uint][]interface{}),
	}
}

func (m *PersistentMemoryTrackingHandler) HandleEvent(event Event) {
	// log.Printf("Event SessionId: %d, ItemId: %d, Position: %f", event.SessionId, event.Item, event.Position)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ItemPopularity[event.Item] += 100.0
	m.updateSession(event, event.SessionId)

	m.changes++
	go opsProcessed.Inc()
}

func (m *PersistentMemoryTrackingHandler) HandleCartEvent(event CartEvent) {
	// log.Printf("Cart event SessionId: %d, ItemId: %d, Quantity: %d", event.SessionId, event.Item, event.Quantity)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ItemPopularity[event.Item] += 190
	m.changes++
	go opsProcessed.Inc()
	m.updateSession(event, event.SessionId)
}

func (m *PersistentMemoryTrackingHandler) HandleSearchEvent(event SearchEventData) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changes++
	go opsProcessed.Inc()
	if event.Query != "" {
		m.Queries[event.Query] += 1
	}
	for _, filter := range event.Filters.StringFilter {
		m.FieldPopularity[filter.Id] += 1
	}
	for _, filter := range event.Filters.RangeFilter {
		m.FieldPopularity[filter.Id] += 1
	}
	m.updateSession(event, event.SessionId)

}

func (m *PersistentMemoryTrackingHandler) updateSession(event interface{}, sessionId int) {

	session, ok := m.Sessions[sessionId]
	needsSync := false
	facetsChanged := false
	itemsChanged := false
	if ok {
		session.Events = append(session.Events, event)
		now := time.Now().Unix()
		needsSync = now-session.LastUpdate > 30
		session.LastUpdate = now
		switch e := event.(type) {
		case Event:
			session.PopularItems[e.Item] += 509
			itemsChanged = true
		case SearchEventData:
			for _, filter := range e.Filters.StringFilter {
				if _, ok := session.PopularFacets[filter.Id]; !ok {
					session.PopularFacets[filter.Id] = make([]interface{}, 0)
				}
				session.PopularFacets[filter.Id] = append(session.PopularFacets[filter.Id], filter.Value)
				facetsChanged = true
			}
			for _, filter := range e.Filters.RangeFilter {
				if _, ok := session.PopularFacets[filter.Id]; !ok {
					session.PopularFacets[filter.Id] = make([]interface{}, 0)
				}
				session.PopularFacets[filter.Id] = append(session.PopularFacets[filter.Id], filter)
				facetsChanged = true
			}
		case ImpressionEvent:
			for _, impression := range e.Items {
				session.PopularItems[impression.Id] += 10
			}
			itemsChanged = true
		case CartEvent:
			session.PopularItems[e.Item] += 150
			itemsChanged = true
		case ActionEvent:
			session.PopularItems[e.Item] += 80
			itemsChanged = true
		case PurchaseEvent:
			for _, purchase := range e.Items {
				session.PopularItems[purchase.Id] += 1000
				itemsChanged = true
			}
		}
		if m.trackingHandler != nil && needsSync {
			if facetsChanged {
				m.trackingHandler.SessionFieldPopularityChanged(sessionId, &session.PopularFacets)
			}
			if itemsChanged {
				m.trackingHandler.SessionPopularityChanged(sessionId, &session.PopularItems)
			}
		}
	}
}

func (m *PersistentMemoryTrackingHandler) HandleImpressionEvent(event ImpressionEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	go opsProcessed.Inc()
	for _, impression := range event.Items {
		m.ItemPopularity[impression.Id] += 0.01 + float64(impression.Position)/1000
	}
	m.updateSession(event, event.SessionId)
	m.changes++

}

func (m *PersistentMemoryTrackingHandler) HandleActionEvent(event ActionEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	go opsProcessed.Inc()
	m.updateSession(event, event.SessionId)
	m.changes++
}
