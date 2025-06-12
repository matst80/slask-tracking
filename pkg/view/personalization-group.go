package view

import (
	"log"
	"time"
)

type PersonalizationGroup struct {
	Id          string    `json:"id"`
	Name        string    `json:"name"`
	ItemEvents  DecayList `json:"item_events"`
	FieldEvents DecayList `json:"field_events"`
	Created     int64     `json:"ts"`
	LastUpdate  int64     `json:"last_update"`
	LastSync    int64     `json:"last_sync"`
}

func (p *PersonalizationGroup) HandleEvent(event interface{}) {
	now := time.Now().Unix()
	if p.FieldEvents == nil {
		p.FieldEvents = make(DecayList)
	}
	if p.ItemEvents == nil {
		p.ItemEvents = make(DecayList)
	}
	if p.Created == 0 {
		p.Created = now
	}
	if p.LastUpdate == 0 {
		p.LastUpdate = now
	}
	switch e := event.(type) {
	case Event:
		if e.BaseItem != nil && e.Id > 0 {
			p.ItemEvents.Add(e.Id, DecayEvent{
				TimeStamp: now,
				Value:     200,
			})

		} else {
			log.Printf("Event without item %+v", event)
		}
		break
	case SearchEvent:
		for _, filter := range e.Filters.StringFilter {
			p.FieldEvents.Add(filter.Id, DecayEvent{
				TimeStamp: now,
				Value:     150,
			})
		}
		for _, filter := range e.Filters.RangeFilter {
			p.FieldEvents.Add(filter.Id, DecayEvent{
				TimeStamp: now,
				Value:     100,
			})
		}
		break
	case ImpressionEvent:
		for _, impression := range e.Items {
			p.ItemEvents.Add(impression.Id, DecayEvent{
				TimeStamp: now,
				Value:     0.02 * float64(max(impression.Position, 300)),
			})
		}
		break
	case CartEvent:
		p.ItemEvents.Add(e.Id, DecayEvent{
			TimeStamp: now,
			Value:     700,
		})
		break
	case ActionEvent:
		if e.BaseItem != nil && e.Id > 0 {
			p.ItemEvents.Add(e.Id, DecayEvent{
				TimeStamp: now,
				Value:     80,
			})
		}
		break
	case PurchaseEvent:
		for _, purchase := range e.Items {
			p.ItemEvents.Add(purchase.Id, DecayEvent{
				TimeStamp: now,
				Value:     800 * float64(purchase.Quantity),
			})
		}
		break
	}
}
