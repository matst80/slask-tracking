package view

import (
	"cmp"
	"log"
	"maps"
	"slices"
	"time"
)

func (session *SessionData) DecayEvents(trk PopularityListener) {
	ts := time.Now().Unix()
	now := ts

	session.LastSync = ts
	sf := len(session.FieldEvents)
	if sf > 0 {
		//log.Printf("Decaying field events %d", sf)
		fieldPopularity := session.FieldEvents.Decay(now)
		//log.Printf("Session field popularity %d", len(session.FieldPopularity))
		if len(fieldPopularity) > 0 {
			if err := trk.SessionFieldPopularityChanged(session.Id, &fieldPopularity); err != nil {
				log.Println(err)
			}
		}
	}

	si := len(session.ItemEvents)
	if si > 0 {
		itemPopularity := session.ItemEvents.Decay(now)
		if len(itemPopularity) > 0 {
			if err := trk.SessionPopularityChanged(session.Id, &itemPopularity); err != nil {
				log.Println(err)
			} else {
				log.Printf("Sending session item events %d", len(itemPopularity))
			}
		}
	}
}

func (p *PersonalizationGroup) DecayGroupEvents(trk PopularityListener) {
	ts := time.Now().Unix()
	now := ts

	p.LastSync = ts
	sf := len(p.FieldEvents)
	if sf > 0 {
		//log.Printf("Decaying field events %d", sf)
		fieldPopularity := p.FieldEvents.Decay(now)
		//log.Printf("Session field popularity %d", len(p.FieldPopularity))
		if len(fieldPopularity) > 0 {
			if err := trk.GroupFieldPopularityChanged(p.Id, &fieldPopularity); err != nil {
				log.Println(err)
			}
		}
	}

	si := len(p.ItemEvents)
	if si > 0 {
		itemPopularity := p.ItemEvents.Decay(now)
		if len(itemPopularity) > 0 {
			if err := trk.GroupPopularityChanged(p.Id, &itemPopularity); err != nil {
				log.Println(err)
			} else {
				log.Printf("Sending group %s item events %d", p.Name, len(itemPopularity))
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

func byValueScore(a, b FacetValueResult) int {
	return cmp.Compare(b.Score, a.Score)
}

func byFacetScore(a, b FacetResult) int {
	return cmp.Compare(b.Score, a.Score)
}

func byQueryScore(a, b QueryResult) int {
	return cmp.Compare(b.Score, a.Score)
}

func (s *PersistentMemoryTrackingHandler) DecaySuggestions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()

	result := make([]QueryResult, 0)

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

	for q, suggestion := range s.QueryEvents {
		suggestion.Popularity.Decay(now)
		queryResult := QueryResult{
			Query: q,
			Score: suggestion.Popularity.Value,
		}
		facetResults := make([]FacetResult, 0)
		for facetId, keyField := range suggestion.KeyFields {
			valueResults := make([]FacetValueResult, 0)
			keyField.FieldPopularity.Decay(now)
			facetResult := FacetResult{
				FacetId: facetId,
				Score:   keyField.FieldPopularity.Value,
			}
			for value, v := range keyField.ValuePopularity {
				v.Decay(now)
				valueResults = append(valueResults, FacetValueResult{
					Value: value,
					Score: v.Value,
				})
			}
			slices.SortFunc(valueResults, byValueScore)

			facetResult.Values = valueResults
			facetResults = append(facetResults, facetResult)
			maps.DeleteFunc(keyField.ValuePopularity, func(key string, value *DecayPopularity) bool {
				// log.Printf("Deleting value popularity %s for query %s, value:%f", key, q, value.Value)
				return value.Value < 0.0002
			})

		}
		maps.DeleteFunc(suggestion.KeyFields, func(key uint, value QueryKeyData) bool {
			// log.Printf("Deleting facet popularity %d for query %s, value:%f", key, q, value.FieldPopularity.Value)
			return value.FieldPopularity.Value < 0.0002
		})
		slices.SortFunc(facetResults, byFacetScore)
		queryResult.Facets = facetResults
		result = append(result, queryResult)
	}

	slices.SortFunc(result, byQueryScore)

	maps.DeleteFunc(s.QueryEvents, func(key string, value QueryMatcher) bool {
		// log.Printf("Deleting query %s, value %f", key, value.Popularity.Value)
		return value.Popularity.Value < 0.0002
	})
	s.SortedQueries = result
	log.Printf("Decayed suggestions %d", len(s.QueryEvents))
}

func (s *PersistentMemoryTrackingHandler) cleanSessions() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.EmptyResults = slices.DeleteFunc(s.EmptyResults, func(i SearchEvent) bool {
		return i.Query == ""
	})
	for id, session := range s.Sessions {
		session.Events = slices.DeleteFunc(session.Events, func(i interface{}) bool {
			return i == nil
		})
		if session.Id != id {
			session.Id = id
		}
	}
	log.Println("Cleaning sessions")

	limit := time.Now().Add(-time.Hour * (24 * 7)).Unix()
	maps.DeleteFunc(s.Sessions, func(key int64, value *SessionData) bool {
		if value == nil {
			return true
		}
		//if value.SessionContent == nil {
		//	log.Printf("Session content is nil for key: %d", key)
		//	return true
		//}
		////if len(value.Events) < 2 {
		////	log.Printf("Session %d has less than 2 events", key)
		////	return true
		////}
		//if value.UserAgent == "" && value.Ip == "" {
		//	log.Printf("Session %d has no user agent or ip", key)
		//	return true
		//}
		//log.Printf("last update %d, limit %d, delete? %v", value.LastUpdate, limit, value.LastUpdate < limit)
		return value.LastUpdate < limit
	})
	// for key, item := range s.Sessions {
	// 	if limit > item.LastUpdate {
	// 		log.Printf("Deleting session %d", key)
	// 		delete(s.Sessions, key)
	// 	}
	// }
}

func (s *PersistentMemoryTrackingHandler) DecayFacetValuesEvents() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().Unix()

	result := map[uint][]FacetValueResult{}

	for facetId, facet := range s.FieldValueEvents {
		valueResult := make([]FacetValueResult, 0)
		score := 0.0
		for value, field := range facet {
			field.Decay(now)
			if field.Value > 0.0002 {
				valueResult = append(valueResult, FacetValueResult{
					Value: value,
					Score: field.Value,
				})
				score += field.Value
			}
		}
		slices.SortFunc(valueResult, byValueScore)
		result[facetId] = valueResult

		maps.DeleteFunc(facet, func(key string, value *DecayPopularity) bool {
			return value.Value < 0.0002
		})
	}
	s.FieldValueScores = result
	log.Printf("Decayed field events %d", len(s.FieldEvents))
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

func (s *PersistentMemoryTrackingHandler) DecayGroupEvents() {
	if s.trackingHandler != nil {
		for id, group := range s.PersonalizationGroups {
			if group.Id != id {
				group.Id = id
			}
			group.DecayGroupEvents(s.trackingHandler)
		}
	}
}
