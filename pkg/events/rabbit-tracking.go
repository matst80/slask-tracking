package events

import (
	"encoding/json"
	"log"
	"time"

	"github.com/matst80/slask-tracking/pkg/view"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitTrackingConfig struct {
	TrackingTopic string
	Url           string
}

type RabbitTransportClient struct {
	RabbitTrackingConfig

	ClientName string
	handler    view.TrackingHandler
	connection *amqp.Connection
	channel    *amqp.Channel
	quit       chan bool
}

func (t *RabbitTransportClient) declareBindAndConsume(topic string) (<-chan amqp.Delivery, error) {
	q, err := t.channel.QueueDeclare(
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
	err = t.channel.QueueBind(q.Name, topic, topic, false, nil)
	if err != nil {
		return nil, err
	}
	return t.channel.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
}

func (t *RabbitTransportClient) Connect(handler view.TrackingHandler) error {
	conn, err := amqp.Dial(t.Url)
	t.quit = make(chan bool)
	if err != nil {
		return err
	}
	t.connection = conn
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	t.handler = handler
	t.channel = ch
	toAdd, err := t.declareBindAndConsume(t.TrackingTopic)
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
					t.handler.HandleSessionEvent(session)
				} else {
					log.Printf("Failed to unmarshal session message %v", err)
				}
			case 1:
				var searchEventData view.SearchEventData
				if err := json.Unmarshal(d.Body, &searchEventData); err == nil {
					if searchEventData.TimeStamp == 0 {
						searchEventData.TimeStamp = time.Now().Unix()
					}
					t.handler.HandleSearchEvent(searchEventData)
				} else {
					log.Printf("Failed to unmarshal search event message %v", err)
				}
			case 2:
				var event view.Event
				if err := json.Unmarshal(d.Body, &event); err == nil {
					t.handler.HandleEvent(event)
				} else {
					log.Printf("Failed to unmarshal event message %v", err)
				}
			case 3:
				var cartEvent view.CartEvent
				if err := json.Unmarshal(d.Body, &cartEvent); err == nil {
					if cartEvent.TimeStamp == 0 {
						cartEvent.TimeStamp = time.Now().Unix()
					}
					t.handler.HandleCartEvent(cartEvent)
				} else {
					log.Printf("Failed to unmarshal cart event message %v", err)
				}
			case 4:
				var cartEvent view.CartEvent
				if err := json.Unmarshal(d.Body, &cartEvent); err == nil {
					if cartEvent.TimeStamp == 0 {
						cartEvent.TimeStamp = time.Now().Unix()
					}
					t.handler.HandleCartEvent(cartEvent)
				} else {
					log.Printf("Failed to unmarshal cart event message %v", err)
				}
			case 5:
				var impressionsEvent view.ImpressionEvent
				if err := json.Unmarshal(d.Body, &impressionsEvent); err == nil {
					if impressionsEvent.TimeStamp == 0 {
						impressionsEvent.TimeStamp = time.Now().Unix()
					}
					t.handler.HandleImpressionEvent(impressionsEvent)
				} else {
					log.Printf("Failed to unmarshal impressions event message %v", err)
				}
			case 6:
				var actionEvent view.ActionEvent
				if err := json.Unmarshal(d.Body, &actionEvent); err == nil {
					if actionEvent.TimeStamp == 0 {
						actionEvent.TimeStamp = time.Now().Unix()
					}
					t.handler.HandleActionEvent(actionEvent)
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

func (t *RabbitTransportClient) Close() {
	if (t.channel != nil) && (!t.channel.IsClosed()) {
		t.channel.Close()
	}
	if (t.connection != nil) && (!t.connection.IsClosed()) {
		t.connection.Close()
	}
	//t.quit <- true

}
