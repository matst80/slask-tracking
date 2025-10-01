package events

import (
	"encoding/json"
	"log"

	"github.com/matst80/slask-finder/pkg/messaging"
	"github.com/matst80/slask-tracking/pkg/view"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ConnectTrackingHandler(ch *amqp.Channel, handler view.TrackingHandler) error {

	return messaging.ListenToTopic(ch, "global", "tracking", func(msg amqp.Delivery) error {
		var event view.BaseEvent
		if err := json.Unmarshal(msg.Body, &event); err == nil {
			switch event.Event {
			case 0:
				var session view.Session
				if err := json.Unmarshal(msg.Body, &session); err == nil {
					session.SetTimestamp()
					handler.HandleSessionEvent(session)
				} else {
					log.Printf("Failed to unmarshal session message %v", err)
				}
			case 1:
				var searchEventData view.SearchEvent
				if err := json.Unmarshal(msg.Body, &searchEventData); err == nil {
					searchEventData.SetTimestamp()
					handler.HandleSearchEvent(searchEventData, nil)
				} else {
					log.Printf("Failed to unmarshal search event message %v", err)
				}
			case 2:
				var event view.Event
				if err := json.Unmarshal(msg.Body, &event); err == nil {
					event.SetTimestamp()
					handler.HandleEvent(event, nil)
				} else {
					log.Printf("Failed to unmarshal event message %v", err)
				}
			case 3:
				var cartEvent view.CartEvent
				if err := json.Unmarshal(msg.Body, &cartEvent); err == nil {
					cartEvent.SetTimestamp()
					handler.HandleCartEvent(cartEvent, nil)
				} else {
					log.Printf("Failed to unmarshal cart event message %v", err)
				}
			case 4:
				var cartEvent view.CartEvent
				if err := json.Unmarshal(msg.Body, &cartEvent); err == nil {
					cartEvent.SetTimestamp()
					handler.HandleCartEvent(cartEvent, nil)
				} else {
					log.Printf("Failed to unmarshal cart event message %v", err)
				}
			case 5:
				var impressionsEvent view.ImpressionEvent
				if err := json.Unmarshal(msg.Body, &impressionsEvent); err == nil {
					impressionsEvent.SetTimestamp()
					handler.HandleImpressionEvent(impressionsEvent, nil)
				} else {
					log.Printf("Failed to unmarshal impressions event message %v", err)
				}
			case 6:
				var actionEvent view.ActionEvent
				if err := json.Unmarshal(msg.Body, &actionEvent); err == nil {
					actionEvent.SetTimestamp()
					handler.HandleActionEvent(actionEvent, nil)
				} else {
					log.Printf("Failed to unmarshal action event message %v", err)
				}
			default:
				log.Printf("Unknown event type %v", event.Event)

			}
		} else {
			log.Printf("Failed to unmarshal upset message %v", err)
		}
		return nil
	})

}
