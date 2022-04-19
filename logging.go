package adapter

import (
	"encoding/json"
	"fmt"
)

// Log value
type Log any

type ActionType uint8

const (
	_ ActionType = iota
	ActionTypeClosed
	ActionTypeReceivingMessage
	ActionTypeReceivedMessage
	ActionTypeChangingMessageVisibilityTimeout
	ActionTypeChangedMessageVisibilityTimeout
	ActionTypeDeletingMessage
	ActionTypeDeletedMessage
)

// fmt.Stringer
func (a ActionType) String() string {
	switch a {
	case ActionTypeClosed:
		return "closed"
	case ActionTypeReceivingMessage:
		return "receiving_message"
	case ActionTypeReceivedMessage:
		return "received_message"
	case ActionTypeChangingMessageVisibilityTimeout:
		return "changing_message_visibility_timeout"
	case ActionTypeChangedMessageVisibilityTimeout:
		return "changed_message_visibility_timeout"
	case ActionTypeDeletingMessage:
		return "deleting_message"
	case ActionTypeDeletedMessage:
		return "deleted_message"
	default:
		return "unknown"
	}
}

// json.Marshaler
func (a ActionType) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

// LogMessage
type LogMessage struct {
	ActionType ActionType `json:"action_type"`
	Detail     any        `json:"detail"`
}

// fmt.Stringer
func (msg LogMessage) String() string {
	if msg.Detail == nil {
		return msg.ActionType.String()
	}
	return fmt.Sprintf("%s: %v", msg.ActionType, msg.Detail)
}
