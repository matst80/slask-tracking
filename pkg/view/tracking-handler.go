package view

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type TrackingHandler interface {
	HandleSessionEvent(event Session)
	HandleEvent(event Event)
	HandleSearchEvent(event SearchEventData)
	HandleCartEvent(event CartEvent)
	HandleImpressionEvent(event ImpressionEvent)
	HandleActionEvent(event ActionEvent)
}

type PersistentMemoryTrackingHandler struct {
	path            string
	mu              sync.Mutex
	changes         uint
	trackingHandler PopularityListener
	ItemPopularity  SortOverride            `json:"item_popularity"`
	Queries         map[string]uint         `json:"queries"`
	Sessions        map[string]*SessionData `json:"sessions"`
	FieldPopularity SortOverride            `json:"field_popularity"`
}

type SessionData struct {
	*Session
	Events []interface{} `json:"events"`
}

func (m *PersistentMemoryTrackingHandler) ConnectPopularityListener(handler PopularityListener) {
	m.trackingHandler = handler
}

func MakeMemoryTrackingHandler(path string) *PersistentMemoryTrackingHandler {
	instance, err := load(path)
	if err != nil {
		instance = &PersistentMemoryTrackingHandler{
			ItemPopularity:  make(SortOverride),
			Queries:         make(map[string]uint),
			Sessions:        make(map[string]*SessionData),
			FieldPopularity: make(SortOverride),
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
	if instance.ItemPopularity == nil {
		instance.ItemPopularity = make(SortOverride)
	}
	if instance.Queries == nil {
		instance.Queries = make(map[string]uint)
	}
	if instance.Sessions == nil {
		instance.Sessions = make(map[string]*SessionData)
	}
	if instance.FieldPopularity == nil {
		instance.FieldPopularity = make(SortOverride)
	}
	return instance
}

func (s *PersistentMemoryTrackingHandler) Save() {
	s.save()
}

func (s *PersistentMemoryTrackingHandler) save() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changes == 0 {
		return nil
	}
	log.Println("Saving tracking data")
	if s.trackingHandler != nil {
		go s.trackingHandler.PopularityChanged(&s.ItemPopularity)
	}
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ItemPopularity
}

func (m *PersistentMemoryTrackingHandler) GetQueries() map[string]uint {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.Queries
}

func (m *PersistentMemoryTrackingHandler) GetSessions() []*SessionData {
	m.mu.Lock()
	defer m.mu.Unlock()
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

func (m *PersistentMemoryTrackingHandler) GetFieldPopularity() SortOverride {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.FieldPopularity
}

func (m *PersistentMemoryTrackingHandler) HandleSessionEvent(event Session) {
	// log.Printf("Session new session event %d", event.SessionId)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changes++
	idString := fmt.Sprintf("%d", event.SessionId)
	events := make([]interface{}, 0)
	m.Sessions[idString] = &SessionData{
		Session: &event,
		Events:  events,
	}
}

func (m *PersistentMemoryTrackingHandler) HandleEvent(event Event) {
	// log.Printf("Event SessionId: %d, ItemId: %d, Position: %f", event.SessionId, event.Item, event.Position)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ItemPopularity[event.Item] += 1
	idString := fmt.Sprintf("%d", event.SessionId)
	session, ok := m.Sessions[idString]
	m.changes++
	if ok {
		session.Events = append(session.Events, event)
	}
}

func (m *PersistentMemoryTrackingHandler) HandleCartEvent(event CartEvent) {
	// log.Printf("Cart event SessionId: %d, ItemId: %d, Quantity: %d", event.SessionId, event.Item, event.Quantity)
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ItemPopularity[event.Item] += 10
	idString := fmt.Sprintf("%d", event.SessionId)
	session, ok := m.Sessions[idString]
	m.changes++
	if ok {
		session.Events = append(session.Events, event)
	}
}

func (m *PersistentMemoryTrackingHandler) HandleSearchEvent(event SearchEventData) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changes++
	if event.Query != "" {
		m.Queries[event.Query] += 1
	}
	for _, filter := range event.Filters.StringFilter {
		m.FieldPopularity[filter.Id] += 1
	}
	for _, filter := range event.Filters.IntegerFilter {
		m.FieldPopularity[filter.Id] += 1
	}
	for _, filter := range event.Filters.NumberFilter {
		m.FieldPopularity[filter.Id] += 1
	}
	idString := fmt.Sprintf("%d", event.SessionId)
	session, ok := m.Sessions[idString]
	if ok {
		session.Events = append(session.Events, event)
	}
}

func (m *PersistentMemoryTrackingHandler) HandleImpressionEvent(event ImpressionEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, impression := range event.Items {
		m.ItemPopularity[impression.Id] += 0.01 + float64(impression.Position)/1000
	}
	m.changes++
	idString := fmt.Sprintf("%d", event.SessionId)
	session, ok := m.Sessions[idString]
	if ok {
		session.Events = append(session.Events, event)
	}
}

func (m *PersistentMemoryTrackingHandler) HandleActionEvent(event ActionEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changes++
	idString := fmt.Sprintf("%d", event.SessionId)
	session, ok := m.Sessions[idString]
	if ok {
		session.Events = append(session.Events, event)
	}
}
