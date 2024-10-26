package events

import (
	"encoding/json"
	"log"
	"time"

	"github.com/matst80/slask-finder/pkg/index"
	"github.com/matst80/slask-tracking/pkg/view"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitTrackingConfig struct {
	TrackingTopic      string
	ItemsUpsertedTopic string
	Url                string
}

type RabbitTransportClient struct {
	RabbitTrackingConfig
}

func (t *RabbitTransportClient) declareBindAndConsume(ch *amqp.Channel, topic string) (<-chan amqp.Delivery, error) {
	q, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}
	err = ch.QueueBind(q.Name, topic, topic, false, nil)
	if err != nil {
		return nil, err
	}
	return ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
}

func (t *RabbitTransportClient) ConnectUpdates(handler view.UpdateHandler) error {

	conn, err := amqp.Dial(t.Url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	toAdd, err := t.declareBindAndConsume(ch, t.ItemsUpsertedTopic)
	if err != nil {
		return err
	}

	for d := range toAdd {
		//log.Printf("Got upsert message")
		var items []interface{}
		if err := json.Unmarshal(d.Body, &items); err == nil {
			handler.HandleUpdate(items)
		} else {
			log.Printf("Failed to unmarshal upset message %v", err)
		}
	}
	return nil
}

func (t *RabbitTransportClient) ConnectPriceUpdates(handler view.PriceUpdateHandler) error {

	conn, err := amqp.Dial(t.Url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	toAdd, err := t.declareBindAndConsume(ch, "price_lowered")
	if err != nil {
		return err
	}

	for d := range toAdd {
		//log.Printf("Got upsert message")
		var items []index.DataItem
		if err := json.Unmarshal(d.Body, &items); err == nil {
			handler.HandlePriceUpdate(items)
		} else {
			log.Printf("Failed to unmarshal upset message %v", err)
		}
	}
	return nil
}

func (t *RabbitTransportClient) Connect(handler view.TrackingHandler) error {
	conn, err := amqp.Dial(t.Url)

	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	toAdd, err := t.declareBindAndConsume(ch, t.TrackingTopic)
	if err != nil {
		return err
	}

	var event view.BaseEvent
	for d := range toAdd {
		log.Printf("Got tracking data %s", string(d.Body))

		if err := json.Unmarshal(d.Body, &event); err == nil {
			switch event.Event {
			case 0:
				var session view.Session
				if err := json.Unmarshal(d.Body, &session); err == nil {
					if session.TimeStamp == 0 {
						session.TimeStamp = time.Now().Unix()
					}
					handler.HandleSessionEvent(session)
				} else {
					log.Printf("Failed to unmarshal session message %v", err)
				}
			case 1:
				var searchEventData view.SearchEventData
				if err := json.Unmarshal(d.Body, &searchEventData); err == nil {
					if searchEventData.TimeStamp == 0 {
						searchEventData.TimeStamp = time.Now().Unix()
					}
					handler.HandleSearchEvent(searchEventData)
				} else {
					log.Printf("Failed to unmarshal search event message %v", err)
				}
			case 2:
				var event view.Event
				if err := json.Unmarshal(d.Body, &event); err == nil {
					handler.HandleEvent(event)
				} else {
					log.Printf("Failed to unmarshal event message %v", err)
				}
			case 3:
				var cartEvent view.CartEvent
				if err := json.Unmarshal(d.Body, &cartEvent); err == nil {
					if cartEvent.TimeStamp == 0 {
						cartEvent.TimeStamp = time.Now().Unix()
					}
					handler.HandleCartEvent(cartEvent)
				} else {
					log.Printf("Failed to unmarshal cart event message %v", err)
				}
			case 4:
				var cartEvent view.CartEvent
				if err := json.Unmarshal(d.Body, &cartEvent); err == nil {
					if cartEvent.TimeStamp == 0 {
						cartEvent.TimeStamp = time.Now().Unix()
					}
					handler.HandleCartEvent(cartEvent)
				} else {
					log.Printf("Failed to unmarshal cart event message %v", err)
				}
			case 5:
				var impressionsEvent view.ImpressionEvent
				if err := json.Unmarshal(d.Body, &impressionsEvent); err == nil {
					if impressionsEvent.TimeStamp == 0 {
						impressionsEvent.TimeStamp = time.Now().Unix()
					}
					handler.HandleImpressionEvent(impressionsEvent)
				} else {
					log.Printf("Failed to unmarshal impressions event message %v", err)
				}
			case 6:
				var actionEvent view.ActionEvent
				if err := json.Unmarshal(d.Body, &actionEvent); err == nil {
					if actionEvent.TimeStamp == 0 {
						actionEvent.TimeStamp = time.Now().Unix()
					}
					handler.HandleActionEvent(actionEvent)
				} else {
					log.Printf("Failed to unmarshal impressions event message %v", err)
				}
			default:
				log.Printf("Unknown event type %v", event.Event)

			}
		} else {
			log.Printf("Failed to unmarshal upset message %v", err)
		}
	}

	return nil
}
