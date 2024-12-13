package view

import (
	"context"
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
