package view

type DecayConfig struct {
	DecayRate float64 `json:"decay_rate"`
	MaxAge    int64   `json:"max_age"`
}

type ValueRule struct {
	EventType          uint16            `json:"event_type"`
	BaseValue          float64           `json:"base_value"`
	PositionMultiplier float64           `json:"position_multiplier"`
	QuantityMultiplier float64           `json:"quantity_multiplier"`
	Conditions         map[string]string `json:"conditions,omitempty"`
}

type PersonaRule struct {
	Persona    string            `json:"persona"`
	Points     float64           `json:"points"`
	Conditions map[string]string `json:"conditions,omitempty"`
}

type TrackingConfig struct {
	Decay        DecayConfig   `json:"decay"`
	Rules        []ValueRule   `json:"rules"`
	PersonaRules []PersonaRule `json:"persona_rules"`
}

func DefaultConfig() TrackingConfig {
	return TrackingConfig{
		Decay: DecayConfig{
			DecayRate: 0.9999995,
			MaxAge:    60 * 60 * 24 * 48, // 48 hours
		},
		Rules: []ValueRule{
			{
				EventType:          EVENT_ITEM_CLICK,
				BaseValue:          200.0,
				PositionMultiplier: 0.1,
			},
			{
				EventType:          CART_ADD,
				QuantityMultiplier: 190.0,
			},
			{
				EventType:          CART_REMOVE,
				QuantityMultiplier: 190.0,
			},
			{
				EventType:          CART_CLEAR,
				QuantityMultiplier: 190.0,
			},
			{
				EventType:          CART_QUANTITY,
				QuantityMultiplier: 190.0,
			},
			{
				EventType:          CART_ENTER_CHECKOUT,
				QuantityMultiplier: 200.0,
			},
			{
				EventType:          EVENT_ITEM_IMPRESS,
				PositionMultiplier: 1.0,
			},
			{
				EventType: EVENT_ITEM_ACTION,
				BaseValue: 30.0,
			},
			{
				EventType: EVENT_SEARCH,
				BaseValue: 20.0,
			},
		},
		PersonaRules: []PersonaRule{
			{
				Persona: "gamer",
				Points:  5.0,
				Conditions: map[string]string{
					"category": "Gaming",
				},
			},
			{
				Persona: "tv",
				Points:  5.0,
				Conditions: map[string]string{
					"category3": "TV",
				},
			},
			{
				Persona: "apple",
				Points:  3.0,
				Conditions: map[string]string{
					"brand": "Apple",
				},
			},
		},
	}
}

func (c *TrackingConfig) CalculateEventValue(eventType uint16, item *BaseItem) float64 {
	// Fallback defaults
	var ruleFound bool
	var selectedRule ValueRule

	for _, rule := range c.Rules {
		if rule.EventType != eventType {
			continue
		}

		if len(rule.Conditions) > 0 && item != nil {
			matched := true
			for k, v := range rule.Conditions {
				switch k {
				case "category":
					if item.Category != v {
						matched = false
					}
				case "brand":
					if item.Brand != v {
						matched = false
					}
				case "name":
					if item.Name != v {
						matched = false
					}
				default:
					matched = false
				}
			}
			if !matched {
				continue
			}
		}

		selectedRule = rule
		ruleFound = true
		break
	}

	if !ruleFound {
		// Provide reasonable default fallbacks if no specific rule is matched
		switch eventType {
		case EVENT_ITEM_CLICK:
			return 200.0
		case CART_ADD, CART_REMOVE, CART_CLEAR, CART_QUANTITY:
			if item != nil && item.Quantity > 0 {
				return 190.0 * float64(item.Quantity)
			}
			return 190.0
		case CART_ENTER_CHECKOUT:
			if item != nil && item.Quantity > 0 {
				return 200.0 * float64(item.Quantity)
			}
			return 200.0
		case EVENT_ITEM_IMPRESS:
			if item != nil {
				return float64(item.Position)
			}
			return 1.0
		case EVENT_ITEM_ACTION:
			return 30.0
		case EVENT_SEARCH:
			return 20.0
		default:
			return 1.0
		}
	}

	value := selectedRule.BaseValue
	if item != nil {
		if selectedRule.PositionMultiplier != 0 {
			pos := item.Position
			// Cap position at 300 to match original min(event.Position, 300) behavior if needed
			if pos > 300 {
				pos = 300
			}
			value += selectedRule.PositionMultiplier * float64(pos)
		}
		if selectedRule.QuantityMultiplier != 0 {
			qty := item.Quantity
			if qty == 0 {
				qty = 1
			}
			value += selectedRule.QuantityMultiplier * float64(qty)
		}
	}
	return value
}
