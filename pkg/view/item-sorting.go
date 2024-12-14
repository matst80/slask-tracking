package view

import (
	"context"
	"fmt"
	"log"

	"github.com/matst80/slask-finder/pkg/index"
	"github.com/redis/go-redis/v9"
)

type SortOverrideStorage struct {
	client *redis.Client
	ctx    context.Context
}

const REDIS_POPULAR_KEY = "_popular"
const REDIS_POPULAR_CHANGE = "popularChange"

const REDIS_FIELD_KEY = "_field"
const REDIS_FIELD_CHANGE = "fieldChange"

const REDIS_SESSION_POPULAR_CHANGE = "sessionChange"
const REDIS_SESSION_FIELD_CHANGE = "sessionFieldChange"

func NewSortOverrideStorage(addr string, password string, db int) *SortOverrideStorage {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &SortOverrideStorage{
		client: rdb,
		ctx:    ctx,
	}
}

func (s *SortOverrideStorage) PopularityChanged(sort *index.SortOverride) error {
	data := sort.ToString()
	_, err := s.client.Set(s.ctx, REDIS_POPULAR_KEY, data, 0).Result()
	if err != nil {
		return err
	}

	_, err = s.client.Publish(s.ctx, REDIS_POPULAR_CHANGE, "external").Result()
	if err == nil {
		log.Println("Published popularity change")
	}
	return err
}

func (s *SortOverrideStorage) FieldPopularityChanged(sort *index.SortOverride) error {
	data := sort.ToString()
	_, err := s.client.Set(s.ctx, REDIS_FIELD_KEY, data, 0).Result()
	if err != nil {
		return err
	}

	_, err = s.client.Publish(s.ctx, REDIS_FIELD_CHANGE, "external").Result()
	if err == nil {
		log.Println("Published field popularity change")
	}
	return err
}

func (s *SortOverrideStorage) SessionPopularityChanged(sessionId int, sort *index.SortOverride) error {
	data := sort.ToString()
	id := fmt.Sprintf("_item_%d", sessionId)
	_, err := s.client.Set(s.ctx, id, data, 0).Result()
	if err != nil {
		return err
	}
	_, err = s.client.Publish(s.ctx, REDIS_SESSION_POPULAR_CHANGE, id).Result()
	if err == nil {
		log.Println("Published session popularity change")
	}
	return err
}

func (s *SortOverrideStorage) SessionFieldPopularityChanged(sessionId int, sort *index.SortOverride) error {
	content := sort.ToString()
	id := fmt.Sprintf("_field_%d", sessionId)
	_, err := s.client.Set(s.ctx, id, content, 0).Result()
	if err != nil {
		return err
	}
	_, err = s.client.Publish(s.ctx, REDIS_SESSION_FIELD_CHANGE, id).Result()
	if err == nil {
		log.Println("Published session field popularity change")
	}
	return err
}
