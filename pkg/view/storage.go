package view

import (
	"encoding/json"
	"fmt"
	"strconv"

	"go.etcd.io/bbolt"
)

type BoltStorage struct {
	db *bbolt.DB
}

var (
	sessionsBucket = []byte("sessions")
	metaBucket     = []byte("metadata")
)

func NewBoltStorage(path string) (*BoltStorage, error) {
	db, err := bbolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(sessionsBucket)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(metaBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	return &BoltStorage{db: db}, nil
}

func (s *BoltStorage) Close() error {
	return s.db.Close()
}

func (s *BoltStorage) SaveSession(session *SessionData) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(sessionsBucket)
		key := []byte(strconv.FormatInt(session.Id, 10))
		val, err := json.Marshal(session)
		if err != nil {
			return err
		}
		return b.Put(key, val)
	})
}

func (s *BoltStorage) GetSession(sessionId int64) (*SessionData, error) {
	var session *SessionData
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(sessionsBucket)
		key := []byte(strconv.FormatInt(sessionId, 10))
		val := b.Get(key)
		if val == nil {
			return nil
		}
		session = &SessionData{}
		return json.Unmarshal(val, session)
	})
	return session, err
}

func (s *BoltStorage) SaveMeta(key string, val interface{}) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(metaBucket)
		bytes, err := json.Marshal(val)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), bytes)
	})
}

func (s *BoltStorage) LoadMeta(key string, target interface{}) error {
	return s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(metaBucket)
		bytes := b.Get([]byte(key))
		if bytes == nil {
			return fmt.Errorf("metadata key %s not found", key)
		}
		return json.Unmarshal(bytes, target)
	})
}

func (s *BoltStorage) GetAllSessions() (map[int64]*SessionData, error) {
	sessions := make(map[int64]*SessionData)
	err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(sessionsBucket)
		return b.ForEach(func(k, v []byte) error {
			id, err := strconv.ParseInt(string(k), 10, 64)
			if err != nil {
				return nil
			}
			session := &SessionData{}
			if err := json.Unmarshal(v, session); err == nil {
				sessions[id] = session
			}
			return nil
		})
	})
	return sessions, err
}

func (s *BoltStorage) DeleteSession(sessionId int64) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(sessionsBucket)
		key := []byte(strconv.FormatInt(sessionId, 10))
		return b.Delete(key)
	})
}
