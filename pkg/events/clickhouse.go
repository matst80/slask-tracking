package events

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/matst80/slask-tracking/pkg/view"
)

// ClickHouseConfig holds DSN and optional database
type ClickHouseConfig struct {
	DSN      string
	Database string
}

type ClickHouseStorage struct {
	conn   driver.Conn
	ready  bool
	cfg    ClickHouseConfig
	insert struct {
		eventsPrepared driver.Batch
	}
}

func NewClickHouseStorageFromEnv() (*ClickHouseStorage, error) {
	cfg := ClickHouseConfig{DSN: os.Getenv("CLICKHOUSE_DSN"), Database: os.Getenv("CLICKHOUSE_DB")}
	if cfg.DSN == "" {
		return nil, nil
	}
	store := &ClickHouseStorage{cfg: cfg}
	if err := store.connect(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *ClickHouseStorage) connect() error {
	ctx := context.Background()
	options, err := ch.ParseDSN(s.cfg.DSN)
	if err != nil {
		return err
	}
	if s.cfg.Database != "" {
		options.Auth.Database = s.cfg.Database
	}
	conn, err := ch.Open(options)
	if err != nil {
		return err
	}
	// ping
	if err := conn.Ping(ctx); err != nil {
		return err
	}
	// create table if not exists (base definition kept simple; projections + indexes added separately so they can be retrofitted)
	ddl := `CREATE TABLE IF NOT EXISTS tracking_events (
		ts DateTime64(3) DEFAULT now(),
		event UInt16,
		session_id Int64,
		item_id UInt32,
		position Float32,
		query String,
		page Int32,
		results UInt32,
		filters String,
		value String,
		cart_type String,
		action String,
		reason String
	) ENGINE = MergeTree
	ORDER BY (event, ts)`
	if err := conn.Exec(ctx, ddl); err != nil {
		return err
	}
	// ensure projections and secondary indexes exist
	if err := s.ensureProjectionsAndIndexes(ctx, conn); err != nil {
		log.Printf("clickhouse: ensure projections/indexes error: %v", err)
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO tracking_events (ts, event, session_id, item_id, position, query, page, results, filters, value, cart_type, action, reason)")
	if err != nil {
		return err
	}
	s.conn = conn
	s.insert.eventsPrepared = batch
	s.ready = true
	go s.flushLoop()
	return nil
}

// ensureProjectionsAndIndexes adds projections and data skipping indexes for common analytic queries
func (s *ClickHouseStorage) ensureProjectionsAndIndexes(ctx context.Context, conn driver.Conn) error {
	// existing projections
	projRows, err := conn.Query(ctx, "SELECT name FROM system.projections WHERE database = currentDatabase() AND table = 'tracking_events'")
	if err != nil {
		return err
	}
	existingProj := map[string]bool{}
	for projRows.Next() {
		var n string
		_ = projRows.Scan(&n)
		existingProj[n] = true
	}
	// add session projection
	if !existingProj["session_idx"] {
		if err := conn.Exec(ctx, "ALTER TABLE tracking_events ADD PROJECTION session_idx (SELECT ts, event, session_id, item_id, position, query, page, results, filters, value, cart_type, action, reason ORDER BY (session_id, ts))"); err != nil {
			log.Printf("clickhouse: add session_idx projection failed: %v", err)
		} else {
			_ = conn.Exec(ctx, "ALTER TABLE tracking_events MATERIALIZE PROJECTION session_idx")
		}
	}
	if !existingProj["item_idx"] {
		if err := conn.Exec(ctx, "ALTER TABLE tracking_events ADD PROJECTION item_idx (SELECT ts, event, item_id, session_id, position, query, page, results, filters, value, cart_type, action, reason WHERE item_id > 0 ORDER BY (item_id, ts))"); err != nil {
			log.Printf("clickhouse: add item_idx projection failed: %v", err)
		} else {
			_ = conn.Exec(ctx, "ALTER TABLE tracking_events MATERIALIZE PROJECTION item_idx")
		}
	}
	// indexes
	idxRows, err := conn.Query(ctx, "SELECT name FROM system.data_skipping_indices WHERE database=currentDatabase() AND table='tracking_events'")
	if err != nil {
		return err
	}
	existingIdx := map[string]bool{}
	for idxRows.Next() {
		var n string
		_ = idxRows.Scan(&n)
		existingIdx[n] = true
	}
	if !existingIdx["idx_session_id"] {
		if err := conn.Exec(ctx, "ALTER TABLE tracking_events ADD INDEX idx_session_id session_id TYPE minmax GRANULARITY 1"); err != nil {
			log.Printf("clickhouse: add idx_session_id failed: %v", err)
		}
	}
	if !existingIdx["idx_item_id"] {
		if err := conn.Exec(ctx, "ALTER TABLE tracking_events ADD INDEX idx_item_id item_id TYPE minmax GRANULARITY 1"); err != nil {
			log.Printf("clickhouse: add idx_item_id failed: %v", err)
		}
	}
	return nil
}

func (s *ClickHouseStorage) flushLoop() {
	t := time.NewTicker(2 * time.Second)
	for range t.C {
		if !s.ready || s.insert.eventsPrepared == nil {
			continue
		}
		// Flush if rows buffered
		if err := s.insert.eventsPrepared.Send(); err == nil {
			batch, err := s.conn.PrepareBatch(context.Background(), "INSERT INTO tracking_events (ts, event, session_id, item_id, position, query, page, results, filters, value, cart_type, action, reason)")
			if err != nil {
				log.Printf("clickhouse: prepare batch failed after flush: %v", err)
				continue
			}
			s.insert.eventsPrepared = batch
		}
	}
}

// Multi handler fan-out interface satisfaction
func (s *ClickHouseStorage) HandleSessionEvent(event view.Session)         { s.insertEvent(&event) }
func (s *ClickHouseStorage) HandleEvent(event view.Event, r *http.Request) { s.insertEvent(&event) }
func (s *ClickHouseStorage) HandleSearchEvent(event view.SearchEvent, r *http.Request) {
	s.insertEvent(&event)
}
func (s *ClickHouseStorage) HandleCartEvent(event view.CartEvent, r *http.Request) {
	s.insertEvent(&event)
}
func (s *ClickHouseStorage) HandleDataSetEvent(event view.DataSetEvent, r *http.Request) {
	s.insertEvent(&event)
}
func (s *ClickHouseStorage) HandleEnterCheckout(event view.EnterCheckoutEvent, r *http.Request) {
	s.insertEvent(&event)
}
func (s *ClickHouseStorage) HandleImpressionEvent(event view.ImpressionEvent, r *http.Request) {
	s.insertEvent(&event)
}
func (s *ClickHouseStorage) HandleActionEvent(event view.ActionEvent, r *http.Request) {
	s.insertEvent(&event)
}
func (s *ClickHouseStorage) HandleSuggestEvent(event view.SuggestEvent, r *http.Request) {
	s.insertEvent(&event)
}
func (s *ClickHouseStorage) GetSession(sessionId int64) *view.SessionData { return nil }

// insertEvent writes a normalized row
func (s *ClickHouseStorage) insertEvent(ev interface{}) {
	if !s.ready || s.insert.eventsPrepared == nil {
		return
	}
	ctx := context.Background()
	appendRow := func(ts int64, event uint16, sessionId int64, itemId uint32, pos float32, query string, page int32, results uint32, filters string, value string, cartType string, action string, reason string) {
		// ignore error - will flush at loop
		_ = s.insert.eventsPrepared.Append(time.Unix(ts, 0), event, sessionId, itemId, pos, query, page, results, filters, value, cartType, action, reason)
	}
	switch e := ev.(type) {
	case *view.Event:
		itemId := uint32(0)
		pos := float32(0)
		if e.BaseItem != nil {
			itemId = uint32(e.BaseItem.Id)
			pos = e.BaseItem.Position
		}
		appendRow(e.TimeStamp, e.Event, e.SessionId, itemId, pos, "", 0, 0, "", "", "", "", "")
	case *view.Session:
		appendRow(e.TimeStamp, e.Event, e.SessionId, 0, 0, "", 0, 0, "", "", "", "", "")
	case *view.SearchEvent:
		filters := ""
		if e.Filters != nil {
			// simple json marshal
			if b, err := json.Marshal(e.Filters); err == nil {
				filters = string(b)
			}
		}
		appendRow(e.TimeStamp, e.Event, e.SessionId, 0, 0, e.Query, int32(e.Page), uint32(e.NumberOfResults), filters, "", "", "", "")
	case *view.CartEvent:
		itemId := uint32(0)
		pos := float32(0)
		if e.BaseItem != nil {
			itemId = uint32(e.BaseItem.Id)
			pos = e.BaseItem.Position
		}
		appendRow(e.TimeStamp, e.Event, e.SessionId, itemId, pos, "", 0, 0, "", "", e.Type, "", "")
	case *view.EnterCheckoutEvent:
		appendRow(e.TimeStamp, e.Event, e.SessionId, 0, 0, "", 0, 0, "", "", "enter_checkout", "", "")
	case *view.ImpressionEvent:
		for _, it := range e.Items {
			appendRow(e.TimeStamp, e.Event, e.SessionId, uint32(it.Id), it.Position, "", 0, 0, "", "", "", "", "")
		}
	case *view.ActionEvent:
		itemId := uint32(0)
		pos := float32(0)
		if e.BaseItem != nil {
			itemId = uint32(e.BaseItem.Id)
			pos = e.BaseItem.Position
		}
		appendRow(e.TimeStamp, e.Event, e.SessionId, itemId, pos, "", 0, 0, "", "", "", e.Action, e.Reason)
	case *view.SuggestEvent:
		appendRow(e.TimeStamp, e.Event, e.SessionId, 0, 0, "", 0, uint32(e.Results), "", e.Value, "", "", "")
	case *view.DataSetEvent:
		appendRow(e.TimeStamp, e.Event, e.SessionId, 0, 0, e.Query, 0, 0, "", "", "", "", "")
	default:
		return
	}
	// ensure batch not nil
	if s.insert.eventsPrepared.Rows() >= 5000 {
		if err := s.insert.eventsPrepared.Send(); err != nil {
			log.Printf("clickhouse flush error: %v", err)
			// attempt new batch
			batch, err2 := s.conn.PrepareBatch(ctx, "INSERT INTO tracking_events (ts, event, session_id, item_id, position, query, page, results, filters, value, cart_type, action, reason)")
			if err2 == nil {
				s.insert.eventsPrepared = batch
			}
		}
	}
}

// MultiTrackingHandler fans out to multiple underlying handlers (memory + clickhouse, etc.)

type MultiTrackingHandler struct {
	Handlers []view.TrackingHandler
}

func (m *MultiTrackingHandler) HandleSessionEvent(event view.Session) {
	for _, h := range m.Handlers {
		h.HandleSessionEvent(event)
	}
}
func (m *MultiTrackingHandler) HandleEvent(event view.Event, r *http.Request) {
	for _, h := range m.Handlers {
		h.HandleEvent(event, r)
	}
}
func (m *MultiTrackingHandler) HandleSearchEvent(event view.SearchEvent, r *http.Request) {
	for _, h := range m.Handlers {
		h.HandleSearchEvent(event, r)
	}
}
func (m *MultiTrackingHandler) HandleCartEvent(event view.CartEvent, r *http.Request) {
	for _, h := range m.Handlers {
		h.HandleCartEvent(event, r)
	}
}
func (m *MultiTrackingHandler) HandleDataSetEvent(event view.DataSetEvent, r *http.Request) {
	for _, h := range m.Handlers {
		h.HandleDataSetEvent(event, r)
	}
}
func (m *MultiTrackingHandler) HandleEnterCheckout(event view.EnterCheckoutEvent, r *http.Request) {
	for _, h := range m.Handlers {
		h.HandleEnterCheckout(event, r)
	}
}
func (m *MultiTrackingHandler) HandleImpressionEvent(event view.ImpressionEvent, r *http.Request) {
	for _, h := range m.Handlers {
		h.HandleImpressionEvent(event, r)
	}
}
func (m *MultiTrackingHandler) HandleActionEvent(event view.ActionEvent, r *http.Request) {
	for _, h := range m.Handlers {
		h.HandleActionEvent(event, r)
	}
}
func (m *MultiTrackingHandler) HandleSuggestEvent(event view.SuggestEvent, r *http.Request) {
	for _, h := range m.Handlers {
		h.HandleSuggestEvent(event, r)
	}
}
func (m *MultiTrackingHandler) GetSession(sessionId int64) *view.SessionData {
	// use first handler that returns non-nil
	for _, h := range m.Handlers {
		if s := h.GetSession(sessionId); s != nil {
			return s
		}
	}
	return nil
}

func NewMultiTrackingHandler(handlers ...view.TrackingHandler) view.TrackingHandler {
	return &MultiTrackingHandler{Handlers: handlers}
}
