package view

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/redis/go-redis/v9"
)

type SortOverride map[uint]float64

func (s *SortOverride) ToString() string {
	ret := ""
	for key, value := range *s {
		ret += fmt.Sprintf("%d:%f,", key, value)
	}
	return ret
}

func (s *SortOverride) FromString(data string) error {
	*s = make(map[uint]float64)
	for _, item := range strings.Split(data, ",") {
		var key uint
		var value float64
		_, err := fmt.Sscanf(item, "%d:%f", &key, &value)
		if err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			return err
		}
		(*s)[key] = value
	}
	return nil
}

type SortOverrideStorage struct {
	client *redis.Client
	ctx    context.Context
}

const REDIS_POPULAR_KEY = "_popular"
const REDIS_POPULAR_CHANGE = "popularChange"

// const REDIS_FIELD_KEY = "_field"
// const REDIS_FIELD_CHANGE = "fieldChange"

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

func (s *SortOverrideStorage) PopularityChanged(sort *SortOverride) error {
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
