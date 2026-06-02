package view

import (
	"context"
	"fmt"
	"log"

	"github.com/matst80/slask-finder/pkg/messaging"
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

func (s *SortOverrideStorage) PopularityChanged(sort *types.SortOverride) error {
	s.diskStorage.PopularityChanged(sort)
	messaging.SendChange(s.conn, "global", "sort_override", types.SortOverrideUpdate{
		Key:  "popular",
		Data: *sort,
	})
	return nil
}

func (s *SortOverrideStorage) FieldPopularityChanged(sort *types.SortOverride) error {
	s.diskStorage.FieldPopularityChanged(sort)
	return messaging.SendChange(s.conn, "global", "field_sort_override", types.SortOverrideUpdate{
		Key:  "popular-fields",
		Data: *sort,
	})
}

// sessionOverrideMsg is the wire format for a per-session override. It mirrors
// types.SortOverrideUpdate plus the Group field the reader consumes
// (slask-finder's SortOverrideUpdate.Group, json:"group"). Defined locally so
// the tracker can emit the field without bumping its slask-finder dependency;
// the field matches by JSON tag on the reader side.
type sessionOverrideMsg struct {
	Key   string               `json:"key"`
	Data  types.SortOverride `json:"data"`
	Group string               `json:"group,omitempty"`
}

func (s *SortOverrideStorage) SessionPopularityChanged(sessionId int64, group string, sort *types.SortOverride) error {
	s.diskStorage.SessionPopularityChanged(sessionId, group, sort)
	// Piggyback the session's current group so the reader can resolve the
	// group layer ("group-<id>") without a separate session→group message.
	return messaging.SendChange(s.conn, "global", "sort_override", sessionOverrideMsg{
		Key:   fmt.Sprintf("session-%d", sessionId),
		Data:  *sort,
		Group: group,
	})
}

func (s *SortOverrideStorage) SessionFieldPopularityChanged(sessionId int64, sort *types.SortOverride) error {
	s.diskStorage.SessionFieldPopularityChanged(sessionId, sort)
	return messaging.SendChange(s.conn, "global", "field_sort_override", types.SortOverrideUpdate{
		Key:  fmt.Sprintf("session-fields-%d", sessionId),
		Data: *sort,
	})
}

func (s *SortOverrideStorage) GroupPopularityChanged(groupId string, sort *types.SortOverride) error {
	s.diskStorage.GroupPopularityChanged(groupId, sort)
	return messaging.SendChange(s.conn, "global", "sort_override", types.SortOverrideUpdate{
		Key:  fmt.Sprintf("group-%s", groupId),
		Data: *sort,
	})
}

func (s *SortOverrideStorage) GroupFieldPopularityChanged(groupId string, sort *types.SortOverride) error {
	s.diskStorage.GroupFieldPopularityChanged(groupId, sort)
	return messaging.SendChange(s.conn, "global", "sort_override", types.SortOverrideUpdate{
		Key:  fmt.Sprintf("group-fields-%s", groupId),
		Data: *sort,
	})
}
