package view

import (
	"log"
	"maps"
	"time"
)

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
	for query, _ := range s.QueryEvents {
		for j := len(query) - 1; j >= 4; j-- {
			key := query[:j]
			if key == query {
				continue
			}
			_, found := s.QueryEvents[key]
			if found {
				delete(s.QueryEvents, query)
			} else {
				break
			}
		}
	}
	for _, suggestion := range s.QueryEvents {
		suggestion.Popularity.Decay(now)
		for _, keyField := range suggestion.KeyFields {
			keyField.FieldPopularity.Decay(now)
			for _, v := range keyField.ValuePopularity {
				v.Decay(now)
			}
			maps.DeleteFunc(keyField.ValuePopularity, func(key string, value *DecayPopularity) bool {
				log.Printf("Deleting value popularity %s for query %s, value:%f", key, suggestion.Query, value.Value)
				return value.Value < 0.0002
			})
		}
		maps.DeleteFunc(suggestion.KeyFields, func(key uint, value QueryKeyData) bool {
			log.Printf("Deleting facet popularity %d for query %s, value:%f", key, suggestion.Query, value.FieldPopularity.Value)
			return value.FieldPopularity.Value < 0.0002
		})
	}
	maps.DeleteFunc(s.QueryEvents, func(key string, value QueryMatcher) bool {
		log.Printf("Deleting query %s, value %f", key, value.Popularity.Value)
		return value.Popularity.Value < 0.0002
	})
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
