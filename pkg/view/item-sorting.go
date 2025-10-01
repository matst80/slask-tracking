package view

import (
	"context"
	"fmt"
	"log"

	"github.com/matst80/slask-finder/pkg/messaging"
	"github.com/matst80/slask-finder/pkg/sorting"
	"github.com/matst80/slask-finder/pkg/types"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SortOverrideStorage struct {
	conn        *amqp.Connection
	ctx         context.Context
	diskStorage *DiskOverrideStorage
}

// const REDIS_POPULAR_KEY = "_popular"
// const REDIS_POPULAR_CHANGE = "popularChange"

// const REDIS_FIELD_KEY = "_field"
// const REDIS_FIELD_CHANGE = "fieldChange"

// const REDIS_SESSION_POPULAR_CHANGE = "sessionChange"
// const REDIS_SESSION_FIELD_CHANGE = "sessionFieldChange"
// const REDIS_GROUP_POPULAR_CHANGE = "groupChange"
// const REDIS_GROUP_FIELD_CHANGE = "groupFieldChange"

func NewSortOverrideStorage(conn *amqp.Connection) *SortOverrideStorage {
	ctx := context.Background()
	diskStorage := DiskPopularityListener("data/overrides")
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Unable to create channel %v", err)
	}
	err = messaging.DefineTopic(ch, "global", "sort_override")
	if err != nil {
		log.Fatalf("Unable to define topic %v", err)
	}
	err = messaging.DefineTopic(ch, "global", "field_sort_override")
	if err != nil {
		log.Fatalf("Unable to define topic %v", err)
	}
	return &SortOverrideStorage{
		conn:        conn,
		ctx:         ctx,
		diskStorage: diskStorage,
	}
}

func (s *SortOverrideStorage) PopularityChanged(sort *sorting.SortOverride) error {
	s.diskStorage.PopularityChanged(sort)
	messaging.SendChange(s.conn, "global", "sort_override", types.SortOverrideUpdate{
		Key:  "popular",
		Data: *sort,
	})
	return nil
}

func (s *SortOverrideStorage) FieldPopularityChanged(sort *sorting.SortOverride) error {
	s.diskStorage.FieldPopularityChanged(sort)
	return messaging.SendChange(s.conn, "global", "field_sort_override", types.SortOverrideUpdate{
		Key:  "popular-fields",
		Data: *sort,
	})
}

func (s *SortOverrideStorage) SessionPopularityChanged(sessionId int64, sort *sorting.SortOverride) error {
	s.diskStorage.SessionPopularityChanged(sessionId, sort)
	return messaging.SendChange(s.conn, "global", "sort_override", types.SortOverrideUpdate{
		Key:  fmt.Sprintf("session-%d", sessionId),
		Data: *sort,
	})
}

func (s *SortOverrideStorage) SessionFieldPopularityChanged(sessionId int64, sort *sorting.SortOverride) error {
	s.diskStorage.SessionFieldPopularityChanged(sessionId, sort)
	return messaging.SendChange(s.conn, "global", "field_sort_override", types.SortOverrideUpdate{
		Key:  fmt.Sprintf("session-fields-%d", sessionId),
		Data: *sort,
	})
}

func (s *SortOverrideStorage) GroupPopularityChanged(groupId string, sort *sorting.SortOverride) error {
	s.diskStorage.GroupPopularityChanged(groupId, sort)
	return messaging.SendChange(s.conn, "global", "sort_override", types.SortOverrideUpdate{
		Key:  fmt.Sprintf("group-%s", groupId),
		Data: *sort,
	})
}

func (s *SortOverrideStorage) GroupFieldPopularityChanged(groupId string, sort *sorting.SortOverride) error {
	s.diskStorage.GroupFieldPopularityChanged(groupId, sort)
	return messaging.SendChange(s.conn, "global", "sort_override", types.SortOverrideUpdate{
		Key:  fmt.Sprintf("group-fields-%s", groupId),
		Data: *sort,
	})
}
