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
			if err := trk.SessionPopularityChanged(session.Id, session.dominantGroup(), &itemPopularity); err != nil {
				log.Println(err)
			} else {
				log.Printf("Sending session item events %d", len(itemPopularity))
			}
		}
	}
}

// dominantGroup returns the highest-weighted group this session belongs to, or
// "" if it isn't classified. Sent alongside the per-session override so the
// reader can resolve the group layer (see SortOverrideUpdate.Group).
func (session *SessionData) dominantGroup() string {
	best := ""
	var bestVal float64
	for g, v := range session.Groups {
		if g != "" && v > bestVal {
			best, bestVal = g, v
		}
	}
	return best
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

func byValueScore(a, b FacetValueResult) int {
	return cmp.Compare(b.Score, a.Score)
}

func byFacetScore(a, b FacetResult) int {
	return cmp.Compare(b.Score, a.Score)
}

func byQueryScore(a, b QueryResult) int {
	return cmp.Compare(b.Score, a.Score)
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
