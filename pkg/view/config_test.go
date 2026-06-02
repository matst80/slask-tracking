package view

import (
	"testing"
)

func TestCalculateEventValue_Defaults(t *testing.T) {
	cfg := DefaultConfig()

	// 1. Click default rule: BaseValue = 200, PositionMultiplier = 0.1
	itemClick := &BaseItem{Position: 10}
	val := cfg.CalculateEventValue(EVENT_ITEM_CLICK, itemClick)
	expectedVal := 200.0 + (0.1 * 10.0)
	if val != expectedVal {
		t.Errorf("Expected click value %f, got %f", expectedVal, val)
	}

	// 2. Click default rule: Capped position at 300
	itemClickCapped := &BaseItem{Position: 400}
	valCapped := cfg.CalculateEventValue(EVENT_ITEM_CLICK, itemClickCapped)
	expectedValCapped := 200.0 + (0.1 * 300.0)
	if valCapped != expectedValCapped {
		t.Errorf("Expected click value capped %f, got %f", expectedValCapped, valCapped)
	}

	// 3. Cart add default rule: QuantityMultiplier = 190
	itemCart := &BaseItem{Quantity: 2}
	valCart := cfg.CalculateEventValue(CART_ADD, itemCart)
	expectedValCart := 190.0 * 2.0
	if valCart != expectedValCart {
		t.Errorf("Expected cart add value %f, got %f", expectedValCart, valCart)
	}
}

func TestCalculateEventValue_Conditions(t *testing.T) {
	cfg := DefaultConfig()

	// Add a conditional rule for Category = "Gaming" under clicks
	cfg.Rules = append([]ValueRule{
		{
			EventType: EVENT_ITEM_CLICK,
			BaseValue: 500.0,
			Conditions: map[string]string{
				"category": "Gaming",
			},
		},
	}, cfg.Rules...)

	// Match conditional rule
	gamingItem := &BaseItem{Category: "Gaming"}
	valGaming := cfg.CalculateEventValue(EVENT_ITEM_CLICK, gamingItem)
	if valGaming != 500.0 {
		t.Errorf("Expected gaming click value 500.0, got %f", valGaming)
	}

	// Fallback to regular click rule for non-gaming items
	normalItem := &BaseItem{Category: "Books", Position: 10}
	valNormal := cfg.CalculateEventValue(EVENT_ITEM_CLICK, normalItem)
	expectedNormal := 200.0 + (0.1 * 10)
	if valNormal != expectedNormal {
		t.Errorf("Expected books click value %f, got %f", expectedNormal, valNormal)
	}
}

func TestCalculateEventValue_FallbackNotConfigured(t *testing.T) {
	cfg := TrackingConfig{} // empty config

	// Clicks fallback defaults
	valClick := cfg.CalculateEventValue(EVENT_ITEM_CLICK, &BaseItem{Position: 10})
	if valClick != 200.0 {
		t.Errorf("Expected click fallback default to be 200.0, got %f", valClick)
	}

	// Cart add fallback defaults
	valCart := cfg.CalculateEventValue(CART_ADD, &BaseItem{Quantity: 3})
	if valCart != 190.0*3 {
		t.Errorf("Expected cart fallback default to be 570.0, got %f", valCart)
	}
}

func BenchmarkCalculateEventValue_Hardcoded(b *testing.B) {
	item := &BaseItem{Position: 10, Quantity: 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Mimic previous hardcoded scoring logic
		_ = 200.0 + (0.1 * float64(item.Position))
	}
}

func BenchmarkCalculateEventValue_RulesEngine(b *testing.B) {
	cfg := DefaultConfig()
	item := &BaseItem{Position: 10, Quantity: 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cfg.CalculateEventValue(EVENT_ITEM_CLICK, item)
	}
}

func TestSessionHandleEvent_PersonaRules(t *testing.T) {
	cfg := DefaultConfig()

	// Customize persona rules
	cfg.PersonaRules = []PersonaRule{
		{
			Persona: "premium_buyer",
			Points:  15.0,
			Conditions: map[string]string{
				"brand": "Apple",
			},
		},
		{
			Persona: "bookworm",
			Points:  8.5,
			Conditions: map[string]string{
				"category": "Books",
			},
		},
	}

	session := &SessionData{}

	// 1. Process Apple Event
	appleEvent := Event{
		BaseEvent: &BaseEvent{Event: EVENT_ITEM_CLICK},
		BaseItem:  &BaseItem{Id: 101, Brand: "Apple"},
	}
	session.HandleEvent(appleEvent, &cfg)

	if score := session.Groups["premium_buyer"]; score != 15.0 {
		t.Errorf("Expected premium_buyer score of 15.0, got %f", score)
	}
	if score := session.Groups["bookworm"]; score != 0.0 {
		t.Errorf("Expected bookworm score of 0.0, got %f", score)
	}

	// 2. Process Books Event
	booksEvent := Event{
		BaseEvent: &BaseEvent{Event: EVENT_ITEM_CLICK},
		BaseItem:  &BaseItem{Id: 102, Category: "Books"},
	}
	session.HandleEvent(booksEvent, &cfg)

	if score := session.Groups["bookworm"]; score != 8.5 {
		t.Errorf("Expected bookworm score of 8.5, got %f", score)
	}
}

