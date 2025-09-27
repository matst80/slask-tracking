package view

import (
	"context"

	"github.com/matst80/slask-finder/pkg/sorting"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SortOverrideStorage struct {
	conn *amqp.Connection
	ctx  context.Context
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

	return &SortOverrideStorage{
		conn: conn,
		ctx:  ctx,
	}
}

func (s *SortOverrideStorage) PopularityChanged(sort *sorting.SortOverride) error {

	// data := sort.ToString()

	// _, err := s.client.Set(s.ctx, REDIS_POPULAR_KEY, data, 0).Result()
	// if err != nil {
	// 	return err
	// }

	// _, err = s.client.Publish(s.ctx, REDIS_POPULAR_CHANGE, "external").Result()
	// if err != nil {
	// 	log.Printf("Error publishing popularity change: %v", err)
	// }
	// return err
	return nil
}

func (s *SortOverrideStorage) FieldPopularityChanged(sort *sorting.SortOverride) error {
	// data := sort.ToString()
	// _, err := s.client.Set(s.ctx, REDIS_FIELD_KEY, data, 0).Result()
	// if err != nil {
	// 	return err
	// }

	// _, err = s.client.Publish(s.ctx, REDIS_FIELD_CHANGE, "external").Result()
	// if err != nil {
	// 	log.Printf("Error publishing field popularity change: %v", err)
	// }
	// return err
	return nil
}

func (s *SortOverrideStorage) SessionPopularityChanged(sessionId int64, sort *sorting.SortOverride) error {
	// data := sort.ToString()
	// id := fmt.Sprintf("_item_%d", sessionId)
	// _, err := s.client.Set(s.ctx, id, data, 0).Result()
	// if err != nil {
	// 	return err
	// }
	// _, err = s.client.Publish(s.ctx, REDIS_SESSION_POPULAR_CHANGE, id).Result()
	// return err
	return nil
}

func (s *SortOverrideStorage) SessionFieldPopularityChanged(sessionId int64, sort *sorting.SortOverride) error {
	// content := sort.ToString()
	// id := fmt.Sprintf("_field_%d", sessionId)
	// _, err := s.client.Set(s.ctx, id, content, 0).Result()
	// if err != nil {
	// 	return err
	// }
	// _, err = s.client.Publish(s.ctx, REDIS_SESSION_FIELD_CHANGE, id).Result()
	// if err != nil {
	// 	log.Printf("Error publishing session field popularity change: %v", err)
	// }
	// return err
	return nil
}

func (s *SortOverrideStorage) GroupPopularityChanged(groupId string, sort *sorting.SortOverride) error {
	// content := sort.ToString()
	// id := fmt.Sprintf("group_items_%s", groupId)
	// _, err := s.client.Set(s.ctx, id, content, 0).Result()
	// if err != nil {
	// 	return err
	// }
	// _, err = s.client.Publish(s.ctx, REDIS_GROUP_POPULAR_CHANGE, id).Result()
	// if err != nil {
	// 	log.Printf("Error publishing group field popularity change: %v", err)
	// }
	// return err
	return nil
}

func (s *SortOverrideStorage) GroupFieldPopularityChanged(groupId string, sort *sorting.SortOverride) error {
	// content := sort.ToString()
	// id := fmt.Sprintf("group_field_%s", groupId)
	// _, err := s.client.Set(s.ctx, id, content, 0).Result()
	// if err != nil {
	// 	return err
	// }
	// _, err = s.client.Publish(s.ctx, REDIS_GROUP_FIELD_CHANGE, id).Result()
	// if err != nil {
	// 	log.Printf("Error publishing group field popularity change: %v", err)
	// }
	// return err
	return nil
}
