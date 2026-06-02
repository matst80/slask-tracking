package view

import (
	"log"
	"maps"
	"slices"
)

type Decayer interface {
	Decay(now int64) error
}

type GlobalEventsDecayer struct {
	handler *PersistentMemoryTrackingHandler
}

func (d *GlobalEventsDecayer) Decay(now int64) error {
	d.handler.mu.Lock()
	defer d.handler.mu.Unlock()
	l := len(d.handler.ItemEvents) + len(d.handler.FieldEvents)
	if l == 0 {
		return nil
	}

	d.handler.ItemPopularity = d.handler.ItemEvents.Decay(now)
	d.handler.FieldPopularity = d.handler.FieldEvents.Decay(now)

	log.Printf("Decayed events %d", l)
	return nil
}

type SuggestionDecayer struct {
	handler *PersistentMemoryTrackingHandler
}

func (d *SuggestionDecayer) Decay(now int64) error {
	d.handler.mu.Lock()
	defer d.handler.mu.Unlock()

	result := make([]QueryResult, 0)

	for query := range d.handler.QueryEvents {
		for j := len(query) - 1; j >= 4; j-- {
			key := query[:j]
			if key == query {
				continue
			}
			_, found := d.handler.QueryEvents[key]
			if found {
				delete(d.handler.QueryEvents, query)
			} else {
				break
			}
		}
	}

	for q, suggestion := range d.handler.QueryEvents {
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
				return value.Value < 0.0002
			})

		}
		maps.DeleteFunc(suggestion.KeyFields, func(key uint32, value QueryKeyData) bool {
			return value.FieldPopularity.Value < 0.0002
		})
		slices.SortFunc(facetResults, byFacetScore)
		queryResult.Facets = facetResults
		result = append(result, queryResult)
	}

	slices.SortFunc(result, byQueryScore)

	maps.DeleteFunc(d.handler.QueryEvents, func(key string, value QueryMatcher) bool {
		return value.Popularity.Value < 0.0002
	})
	d.handler.SortedQueries = result
	log.Printf("Decayed suggestions %d", len(d.handler.QueryEvents))
	return nil
}

type SessionDecayer struct {
	handler *PersistentMemoryTrackingHandler
}

func (d *SessionDecayer) Decay(now int64) error {
	if d.handler.trackingHandler != nil {
		for id, session := range d.handler.Sessions {
			if session.Id != id {
				session.Id = id
			}
			session.DecayEvents(d.handler.trackingHandler)
		}
	}
	return nil
}

type GroupDecayer struct {
	handler *PersistentMemoryTrackingHandler
}

func (d *GroupDecayer) Decay(now int64) error {
	if d.handler.trackingHandler != nil {
		for id, group := range d.handler.PersonalizationGroups {
			if group.Id != id {
				group.Id = id
			}
			group.DecayGroupEvents(d.handler.trackingHandler)
		}
	}
	return nil
}

type FacetValuesDecayer struct {
	handler *PersistentMemoryTrackingHandler
}

func (d *FacetValuesDecayer) Decay(now int64) error {
	d.handler.mu.Lock()
	defer d.handler.mu.Unlock()

	result := map[uint32][]FacetValueResult{}

	for facetId, facet := range d.handler.FieldValueEvents {
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
	d.handler.FieldValueScores = result
	log.Printf("Decayed field events %d", len(d.handler.FieldEvents))
	return nil
}
