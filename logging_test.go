package adapter

import (
	"encoding/json"
	"testing"
)

func toJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func TestActionType_String(t *testing.T) {
	type test struct {
		testcase   string
		actionType ActionType
		want       string
	}

	do := func(tt test) {
		t.Run(tt.testcase, func(t *testing.T) {
			got := tt.actionType.String()
			want := tt.want

			if want != got {
				t.Fatalf("want=%v, got=%v.", want, got)
			}
		})
	}

	tests := []test{
		{
			actionType: 0,
			want:       "unknown",
		},
		{
			actionType: ActionTypeClosed,
			want:       "closed",
		},
		{
			actionType: ActionTypeReceivingMessage,
			want:       "receiving_message",
		},
		{
			actionType: ActionTypeReceivedMessage,
			want:       "received_message",
		},
		{
			actionType: ActionTypeChangingMessageVisibilityTimeout,
			want:       "changing_message_visibility_timeout",
		},
		{
			actionType: ActionTypeChangedMessageVisibilityTimeout,
			want:       "changed_message_visibility_timeout",
		},
		{
			actionType: ActionTypeDeletingMessage,
			want:       "deleting_message",
		},
		{
			actionType: ActionTypeDeletedMessage,
			want:       "deleted_message",
		},
		{
			actionType: 255,
			want:       "unknown",
		},
	}

	for _, tt := range tests {
		do(tt)
	}
}

func TestLogMessage_String(t *testing.T) {
	type test struct {
		testcase   string
		logMessage LogMessage
		want       string
		wantJSON   string
	}

	do := func(tt test) {
		t.Run(tt.testcase, func(t *testing.T) {
			got := tt.logMessage.String()
			want := tt.want

			if got != want {
				t.Fatalf("want=%v, got=%v.", want, got)
			}

			got = string(toJSON(tt.logMessage))
			want = tt.wantJSON

			if want != got {
				t.Fatalf("want=%v, got=%v.", want, got)
			}
		})
	}

	tests := []test{
		{
			logMessage: LogMessage{
				ActionType: ActionTypeClosed,
				Detail:     nil,
			},
			want:     "closed",
			wantJSON: `{"action_type":"closed","detail":null}`,
		},
		{
			logMessage: LogMessage{
				ActionType: ActionTypeReceivedMessage,
				Detail:     "foo",
			},
			want:     "received_message: foo",
			wantJSON: `{"action_type":"received_message","detail":"foo"}`,
		},
		{
			logMessage: LogMessage{
				ActionType: 0,
				Detail:     0,
			},
			want:     "unknown: 0",
			wantJSON: `{"action_type":"unknown","detail":0}`,
		},
	}

	for _, tt := range tests {
		do(tt)
	}
}
