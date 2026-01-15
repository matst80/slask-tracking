package view

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

type ClickhouseConfig struct {
	Addresses     []string
	Database      string
	Username      string
	Password      string
	Secure        bool
	SkipVerifyTLS bool
	EventsTable   string
	SessionsTable string
	DialTimeout   time.Duration
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
}

type ClickhouseTrackingHandler struct {
	conn    clickhouse.Conn
	cfg     ClickhouseConfig
	baseCtx context.Context
}

type clickhouseEventRow struct {
	eventTime       time.Time
	sessionID       int64
	eventType       uint16
	eventName       string
	eventValue      float64
	country         string
	context         string
	itemID          uint32
	itemPosition    float32
	itemPrice       float32
	itemQuantity    uint32
	itemBrand       string
	itemCategories  []string
	action          string
	cartType        string
	query           string
	numberOfResults int32
	suggestions     int32
	results         int32
	filtersJSON     string
	referer         string
	ip              string
	userAgent       string
	payloadJSON     string
}

type clickhouseSessionRow struct {
	sessionID int64
	startedAt time.Time
	country   string
	context   string
	ip        string
	userAgent string
	language  string
	referrer  string
	pragma    string
}

var clickhouseEventTypeNames = map[uint16]string{
	EVENT_SESSION_START: "session_start",
	EVENT_ITEM_CLICK:    "item_click",
	EVENT_ITEM_IMPRESS:  "item_impression",
	EVENT_ITEM_ACTION:   "item_action",
	EVENT_SUGGEST:       "suggest",
	EVENT_DATA_SET:      "data_set",
	EVENT_SEARCH:        "search",
	CART_ADD:            "cart_add",
	CART_REMOVE:         "cart_remove",
	CART_CLEAR:          "cart_clear",
	CART_ENTER_CHECKOUT: "cart_enter_checkout",
	CART_QUANTITY:       "cart_quantity",
}

func NewClickhouseTrackingHandler(ctx context.Context, cfg ClickhouseConfig) (*ClickhouseTrackingHandler, error) {
	cfg.applyDefaults()
	if len(cfg.Addresses) == 0 {
		return nil, errors.New("clickhouse addresses must be provided")
	}

	if ctx == nil {
		ctx = context.Background()
	}

	options := &clickhouse.Options{
		Addr: cfg.Addresses,
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:      cfg.DialTimeout,
		MaxOpenConns:     5,
		MaxIdleConns:     5,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	if cfg.Secure {
		options.TLS = &tls.Config{InsecureSkipVerify: cfg.SkipVerifyTLS}
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse connection: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ReadTimeout)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	handler := &ClickhouseTrackingHandler{
		conn:    conn,
		cfg:     cfg,
		baseCtx: ctx,
	}

	if err := handler.ensureSchema(ctx); err != nil {
		return nil, err
	}

	return handler, nil
}

func (cfg *ClickhouseConfig) applyDefaults() {
	filtered := make([]string, 0, len(cfg.Addresses))
	for _, addr := range cfg.Addresses {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		filtered = append(filtered, addr)
	}
	cfg.Addresses = filtered

	if cfg.Database == "" {
		cfg.Database = "tracking"
	}
	if cfg.EventsTable == "" {
		cfg.EventsTable = "events"
	}
	if cfg.SessionsTable == "" {
		cfg.SessionsTable = "sessions"
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 5 * time.Second
	}
}

func (c *ClickhouseTrackingHandler) ensureSchema(ctx context.Context) error {
	if err := c.conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdentifier(c.cfg.Database))); err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	createEventsSQL := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s.%s (
            event_time DateTime('UTC'),
            event_date Date DEFAULT toDate(event_time),
            session_id Int64,
            event_type UInt16,
            event_name LowCardinality(String),
            event_value Float64,
            country LowCardinality(String),
            context String,
            item_id UInt32,
            item_position Float32,
            item_price Float32,
            item_quantity UInt32,
            item_brand LowCardinality(String),
            item_categories Array(LowCardinality(String)),
            action LowCardinality(String),
            cart_type LowCardinality(String),
            query String,
            number_of_results Int32,
            suggestions Int32,
            results Int32,
            filters_json String,
            referer String,
            ip String,
            user_agent String,
            payload_json String,
            inserted_at DateTime('UTC') DEFAULT now('UTC')
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(event_date)
        ORDER BY (event_date, event_type, item_id, session_id)
        SETTINGS index_granularity = 8192
    `, quoteIdentifier(c.cfg.Database), quoteIdentifier(c.cfg.EventsTable))

	if err := c.conn.Exec(ctx, createEventsSQL); err != nil {
		return fmt.Errorf("create events table: %w", err)
	}

	createSessionsSQL := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s.%s (
            session_id Int64,
            started_at DateTime('UTC'),
            country LowCardinality(String),
            context String,
            ip String,
            user_agent String,
            language LowCardinality(String),
            referrer String,
            pragma String,
            inserted_at DateTime('UTC') DEFAULT now('UTC')
        ) ENGINE = ReplacingMergeTree(inserted_at)
        ORDER BY (session_id)
        SETTINGS index_granularity = 8192
    `, quoteIdentifier(c.cfg.Database), quoteIdentifier(c.cfg.SessionsTable))

	if err := c.conn.Exec(ctx, createSessionsSQL); err != nil {
		return fmt.Errorf("create sessions table: %w", err)
	}

	return nil
}

func (c *ClickhouseTrackingHandler) HandleSessionEvent(event Session) {
	if c.conn == nil {
		return
	}
	row := c.sessionRowFromEvent(event)
	c.insertSession(row)

	eventRow := c.eventRowFromSession(event)
	c.insertEventRows([]clickhouseEventRow{eventRow})
}

func (c *ClickhouseTrackingHandler) HandleEvent(event Event, r *http.Request) {
	if c.conn == nil {
		return
	}
	row, ok := c.eventRowFromItemEvent(event.BaseEvent, event.BaseItem, event.Event, event, r, "", "")
	if !ok {
		return
	}
	row.eventValue = 1
	c.insertEventRows([]clickhouseEventRow{row})
}

func (c *ClickhouseTrackingHandler) HandleSearchEvent(event SearchEvent, r *http.Request) {
	if c.conn == nil {
		return
	}
	filters := marshalJSON(event.Filters)
	payload := marshalJSON(event)
	base := ensureBaseEvent(event.BaseEvent)

	sessionContent := requestSessionContent(r)
	row := clickhouseEventRow{
		eventTime:       time.Unix(base.TimeStamp, 0).UTC(),
		sessionID:       base.SessionId,
		eventType:       base.Event,
		eventName:       eventNameFromType(base.Event),
		eventValue:      1,
		country:         base.Country,
		context:         base.Context,
		itemID:          0,
		itemPosition:    0,
		itemPrice:       0,
		itemQuantity:    0,
		itemBrand:       "",
		itemCategories:  nil,
		action:          "",
		cartType:        "",
		query:           event.Query,
		numberOfResults: int32(event.NumberOfResults),
		suggestions:     0,
		results:         0,
		filtersJSON:     filters,
		referer:         sessionContent.Referrer,
		ip:              sessionContent.Ip,
		userAgent:       sessionContent.UserAgent,
		payloadJSON:     payload,
	}
	c.insertEventRows([]clickhouseEventRow{row})
}

func (c *ClickhouseTrackingHandler) HandleCartEvent(event CartEvent, r *http.Request) {
	if c.conn == nil {
		return
	}
	row, ok := c.eventRowFromItemEvent(event.BaseEvent, event.BaseItem, event.Event, event, r, event.Type, "")
	if !ok {
		return
	}
	row.cartType = event.Type
	row.eventValue = float64(event.Quantity)
	c.insertEventRows([]clickhouseEventRow{row})
}

func (c *ClickhouseTrackingHandler) HandleDataSetEvent(event DataSetEvent, r *http.Request) {
	if c.conn == nil {
		return
	}
	payload := marshalJSON(event)
	base := ensureBaseEvent(event.BaseEvent)
	sessionContent := requestSessionContent(r)

	row := clickhouseEventRow{
		eventTime:       time.Unix(base.TimeStamp, 0).UTC(),
		sessionID:       base.SessionId,
		eventType:       base.Event,
		eventName:       eventNameFromType(base.Event),
		eventValue:      1,
		country:         base.Country,
		context:         base.Context,
		itemID:          0,
		itemPosition:    0,
		itemPrice:       0,
		itemQuantity:    0,
		itemBrand:       "",
		itemCategories:  nil,
		action:          "",
		cartType:        "",
		query:           event.Query,
		numberOfResults: 0,
		suggestions:     0,
		results:         0,
		filtersJSON:     "",
		referer:         sessionContent.Referrer,
		ip:              sessionContent.Ip,
		userAgent:       sessionContent.UserAgent,
		payloadJSON:     payload,
	}
	c.insertEventRows([]clickhouseEventRow{row})
}

func (c *ClickhouseTrackingHandler) HandleEnterCheckout(event EnterCheckoutEvent, r *http.Request) {
	if c.conn == nil {
		return
	}
	rows := make([]clickhouseEventRow, 0, len(event.Items))
	for _, item := range event.Items {
		itemCopy := item
		row, ok := c.eventRowFromItemEvent(event.BaseEvent, &itemCopy, event.Event, event, r, "", "checkout")
		if !ok {
			continue
		}
		row.cartType = "checkout"
		row.eventValue = float64(item.Quantity)
		rows = append(rows, row)
	}
	if len(rows) > 0 {
		c.insertEventRows(rows)
	}
}

func (c *ClickhouseTrackingHandler) HandleImpressionEvent(event ImpressionEvent, r *http.Request) {
	if c.conn == nil {
		return
	}
	rows := make([]clickhouseEventRow, 0, len(event.Items))
	for _, item := range event.Items {
		itemCopy := item
		row, ok := c.eventRowFromItemEvent(event.BaseEvent, &itemCopy, event.Event, event, r, "", "")
		if !ok {
			continue
		}
		row.eventValue = 1
		rows = append(rows, row)
	}
	if len(rows) > 0 {
		c.insertEventRows(rows)
	}
}

func (c *ClickhouseTrackingHandler) HandleActionEvent(event ActionEvent, r *http.Request) {
	if c.conn == nil {
		return
	}
	row, ok := c.eventRowFromItemEvent(event.BaseEvent, event.BaseItem, event.Event, event, r, event.Action, "")
	if !ok {
		return
	}
	row.action = event.Action
	row.eventValue = 1
	if event.Referer != "" {
		row.referer = event.Referer
	}
	if event.Reason != "" {
		row.payloadJSON = marshalJSON(event)
	}
	c.insertEventRows([]clickhouseEventRow{row})
}

func (c *ClickhouseTrackingHandler) HandleSuggestEvent(event SuggestEvent, r *http.Request) {
	if c.conn == nil {
		return
	}
	base := ensureBaseEvent(event.BaseEvent)
	sessionContent := requestSessionContent(r)
	payload := marshalJSON(event)
	row := clickhouseEventRow{
		eventTime:       time.Unix(base.TimeStamp, 0).UTC(),
		sessionID:       base.SessionId,
		eventType:       base.Event,
		eventName:       eventNameFromType(base.Event),
		eventValue:      1,
		country:         base.Country,
		context:         base.Context,
		itemID:          0,
		itemPosition:    0,
		itemPrice:       0,
		itemQuantity:    0,
		itemBrand:       "",
		itemCategories:  nil,
		action:          "",
		cartType:        "",
		query:           event.Value,
		numberOfResults: 0,
		suggestions:     int32(event.Suggestions),
		results:         int32(event.Results),
		filtersJSON:     "",
		referer:         sessionContent.Referrer,
		ip:              sessionContent.Ip,
		userAgent:       sessionContent.UserAgent,
		payloadJSON:     payload,
	}
	c.insertEventRows([]clickhouseEventRow{row})
}

func (c *ClickhouseTrackingHandler) GetSession(sessionId int64) *SessionData {
	return nil
}

func (c *ClickhouseTrackingHandler) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *ClickhouseTrackingHandler) eventRowFromItemEvent(base *BaseEvent, item *BaseItem, eventType uint16, payload interface{}, r *http.Request, action string, cart string) (clickhouseEventRow, bool) {
	base = ensureBaseEvent(base)
	if base == nil {
		return clickhouseEventRow{}, false
	}
	eventTime := time.Unix(base.TimeStamp, 0).UTC()
	sessionContent := requestSessionContent(r)
	payloadJSON := marshalJSON(payload)

	var itemID uint32
	var itemPosition float32
	var itemPrice float32
	var itemQuantity uint32
	var itemBrand string
	var categories []string
	if item != nil {
		itemID = uint32(item.Id)
		itemPosition = item.Position
		itemPrice = item.Price
		itemQuantity = uint32(item.Quantity)
		itemBrand = item.Brand
		categories = categoriesFromItem(*item)
	}

	row := clickhouseEventRow{
		eventTime:       eventTime,
		sessionID:       base.SessionId,
		eventType:       eventType,
		eventName:       eventNameFromType(eventType),
		eventValue:      0,
		country:         base.Country,
		context:         base.Context,
		itemID:          itemID,
		itemPosition:    itemPosition,
		itemPrice:       itemPrice,
		itemQuantity:    itemQuantity,
		itemBrand:       itemBrand,
		itemCategories:  categories,
		action:          action,
		cartType:        cart,
		query:           "",
		numberOfResults: 0,
		suggestions:     0,
		results:         0,
		filtersJSON:     "",
		referer:         sessionContent.Referrer,
		ip:              sessionContent.Ip,
		userAgent:       sessionContent.UserAgent,
		payloadJSON:     payloadJSON,
	}
	return row, true
}

func (c *ClickhouseTrackingHandler) insertEventRows(rows []clickhouseEventRow) {
	if len(rows) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(c.baseCtx, c.cfg.WriteTimeout)
	defer cancel()

	stmt := fmt.Sprintf("INSERT INTO %s.%s (event_time, session_id, event_type, event_name, event_value, country, context, item_id, item_position, item_price, item_quantity, item_brand, item_categories, action, cart_type, query, number_of_results, suggestions, results, filters_json, referer, ip, user_agent, payload_json) VALUES", quoteIdentifier(c.cfg.Database), quoteIdentifier(c.cfg.EventsTable))
	batch, err := c.conn.PrepareBatch(ctx, stmt)
	if err != nil {
		log.Printf("clickhouse: prepare batch failed: %v", err)
		return
	}
	for _, row := range rows {
		if err := batch.Append(
			row.eventTime,
			row.sessionID,
			row.eventType,
			row.eventName,
			row.eventValue,
			row.country,
			row.context,
			row.itemID,
			row.itemPosition,
			row.itemPrice,
			row.itemQuantity,
			row.itemBrand,
			row.itemCategories,
			row.action,
			row.cartType,
			row.query,
			row.numberOfResults,
			row.suggestions,
			row.results,
			row.filtersJSON,
			row.referer,
			row.ip,
			row.userAgent,
			row.payloadJSON,
		); err != nil {
			log.Printf("clickhouse: append event failed: %v", err)
			return
		}
	}
	if err := batch.Send(); err != nil {
		log.Printf("clickhouse: send batch failed: %v", err)
	}
}

func (c *ClickhouseTrackingHandler) insertSession(row clickhouseSessionRow) {
	ctx, cancel := context.WithTimeout(c.baseCtx, c.cfg.WriteTimeout)
	defer cancel()

	stmt := fmt.Sprintf("INSERT INTO %s.%s (session_id, started_at, country, context, ip, user_agent, language, referrer, pragma) VALUES", quoteIdentifier(c.cfg.Database), quoteIdentifier(c.cfg.SessionsTable))
	batch, err := c.conn.PrepareBatch(ctx, stmt)
	if err != nil {
		log.Printf("clickhouse: prepare session batch failed: %v", err)
		return
	}
	if err := batch.Append(
		row.sessionID,
		row.startedAt,
		row.country,
		row.context,
		row.ip,
		row.userAgent,
		row.language,
		row.referrer,
		row.pragma,
	); err != nil {
		log.Printf("clickhouse: append session failed: %v", err)
		return
	}
	if err := batch.Send(); err != nil {
		log.Printf("clickhouse: send session batch failed: %v", err)
	}
}

func (c *ClickhouseTrackingHandler) sessionRowFromEvent(event Session) clickhouseSessionRow {
	base := ensureBaseEvent(event.BaseEvent)
	eventTime := time.Now().UTC()
	sessionID := int64(0)
	country := ""
	contextValue := ""
	if base != nil {
		eventTime = time.Unix(base.TimeStamp, 0).UTC()
		sessionID = base.SessionId
		country = base.Country
		contextValue = base.Context
	}
	sc := event.SessionContent
	return clickhouseSessionRow{
		sessionID: sessionID,
		startedAt: eventTime,
		country:   country,
		context:   contextValue,
		ip:        sc.Ip,
		userAgent: sc.UserAgent,
		language:  sc.Language,
		referrer:  sc.Referrer,
		pragma:    sc.PragmaHeader,
	}
}

func (c *ClickhouseTrackingHandler) eventRowFromSession(event Session) clickhouseEventRow {
	base := ensureBaseEvent(event.BaseEvent)
	payload := marshalJSON(event)
	sc := event.SessionContent
	eventTime := time.Now().UTC()
	sessionID := int64(0)
	country := ""
	contextValue := ""
	if base != nil {
		eventTime = time.Unix(base.TimeStamp, 0).UTC()
		sessionID = base.SessionId
		country = base.Country
		contextValue = base.Context
	}

	return clickhouseEventRow{
		eventTime:       eventTime,
		sessionID:       sessionID,
		eventType:       EVENT_SESSION_START,
		eventName:       eventNameFromType(EVENT_SESSION_START),
		eventValue:      1,
		country:         country,
		context:         contextValue,
		itemID:          0,
		itemPosition:    0,
		itemPrice:       0,
		itemQuantity:    0,
		itemBrand:       "",
		itemCategories:  nil,
		action:          "",
		cartType:        "",
		query:           "",
		numberOfResults: 0,
		suggestions:     0,
		results:         0,
		filtersJSON:     "",
		referer:         sc.Referrer,
		ip:              sc.Ip,
		userAgent:       sc.UserAgent,
		payloadJSON:     payload,
	}
}

func ensureBaseEvent(base *BaseEvent) *BaseEvent {
	if base == nil {
		return &BaseEvent{TimeStamp: time.Now().Unix()}
	}
	base.SetTimestamp()
	return base
}

func categoriesFromItem(item BaseItem) []string {
	categories := make([]string, 0, 5)
	if item.Category != "" {
		categories = append(categories, item.Category)
	}
	if item.Category2 != "" {
		categories = append(categories, item.Category2)
	}
	if item.Category3 != "" {
		categories = append(categories, item.Category3)
	}
	if item.Category4 != "" {
		categories = append(categories, item.Category4)
	}
	if item.Category5 != "" {
		categories = append(categories, item.Category5)
	}
	return categories
}

func eventNameFromType(t uint16) string {
	if name, ok := clickhouseEventTypeNames[t]; ok {
		return name
	}
	return fmt.Sprintf("event_%d", t)
}

func marshalJSON(v interface{}) string {
	if v == nil {
		return ""
	}
	data, err := json.Marshal(v)
	if err != nil {
		log.Printf("clickhouse: marshal json failed: %v", err)
		return ""
	}
	return string(data)
}

func requestSessionContent(r *http.Request) SessionContent {
	scPtr := GetSessionContentFromRequest(r)
	if scPtr == nil {
		return SessionContent{}
	}
	return *scPtr
}

func safeCountry(base *BaseEvent) string {
	if base == nil {
		return ""
	}
	return base.Country
}

func safeContext(base *BaseEvent) string {
	if base == nil {
		return ""
	}
	return base.Context
}

func quoteIdentifier(identifier string) string {
	escaped := strings.ReplaceAll(identifier, "`", "``")
	return "`" + escaped + "`"
}
