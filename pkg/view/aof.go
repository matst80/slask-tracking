package view

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
)

type AOFRecord struct {
	Type uint16          `json:"type"`
	Body json.RawMessage `json:"body"`
}

type AOFLogger struct {
	file *os.File
	w    *bufio.Writer
	mu   sync.Mutex
	path string
}

func NewAOFLogger(path string) (*AOFLogger, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}
	return &AOFLogger{
		file: file,
		w:    bufio.NewWriter(file),
		path: path,
	}, nil
}

func (a *AOFLogger) LogEvent(eventType uint16, event interface{}) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	record := AOFRecord{
		Type: eventType,
		Body: body,
	}

	recordBytes, err := json.Marshal(record)
	if err != nil {
		return err
	}

	if _, err := a.w.Write(recordBytes); err != nil {
		return err
	}
	if err := a.w.WriteByte('\n'); err != nil {
		return err
	}

	return a.w.Flush()
}

func (a *AOFLogger) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.w != nil {
		a.w.Flush()
	}
	if a.file != nil {
		return a.file.Close()
	}
	return nil
}

func (a *AOFLogger) Truncate() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.w != nil {
		a.w.Flush()
	}
	if a.file != nil {
		a.file.Close()
	}

	file, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	a.file = file
	a.w = bufio.NewWriter(file)
	return nil
}

func ReplayAOF(path string, handler TrackingHandler) error {
	file, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	lineCount := 0

	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if len(line) == 0 {
			continue
		}

		var record AOFRecord
		if err := json.Unmarshal(line, &record); err != nil {
			log.Printf("AOF replay: failed to unmarshal line: %v", err)
			continue
		}

		lineCount++
		switch record.Type {
		case EVENT_SESSION_START:
			var session Session
			if err := json.Unmarshal(record.Body, &session); err == nil {
				handler.HandleSessionEvent(session)
			}
		case EVENT_SEARCH:
			var searchEvent SearchEvent
			if err := json.Unmarshal(record.Body, &searchEvent); err == nil {
				handler.HandleSearchEvent(searchEvent, nil)
			}
		case EVENT_ITEM_CLICK:
			var event Event
			if err := json.Unmarshal(record.Body, &event); err == nil {
				handler.HandleEvent(event, nil)
			}
		case CART_ADD, CART_REMOVE, CART_CLEAR, CART_QUANTITY:
			var cartEvent CartEvent
			if err := json.Unmarshal(record.Body, &cartEvent); err == nil {
				handler.HandleCartEvent(cartEvent, nil)
			}
		case EVENT_ITEM_IMPRESS:
			var impressionsEvent ImpressionEvent
			if err := json.Unmarshal(record.Body, &impressionsEvent); err == nil {
				handler.HandleImpressionEvent(impressionsEvent, nil)
			}
		case EVENT_ITEM_ACTION:
			var actionEvent ActionEvent
			if err := json.Unmarshal(record.Body, &actionEvent); err == nil {
				handler.HandleActionEvent(actionEvent, nil)
			}
		case EVENT_SUGGEST:
			var suggestEvent SuggestEvent
			if err := json.Unmarshal(record.Body, &suggestEvent); err == nil {
				handler.HandleSuggestEvent(suggestEvent, nil)
			}
		case EVENT_DATA_SET:
			var dataSetEvent DataSetEvent
			if err := json.Unmarshal(record.Body, &dataSetEvent); err == nil {
				handler.HandleDataSetEvent(dataSetEvent, nil)
			}
		case CART_ENTER_CHECKOUT:
			var enterCheckoutEvent EnterCheckoutEvent
			if err := json.Unmarshal(record.Body, &enterCheckoutEvent); err == nil {
				handler.HandleEnterCheckout(enterCheckoutEvent, nil)
			}
		}
	}

	log.Printf("AOF replay completed: replayed %d events", lineCount)
	return nil
}
