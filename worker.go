package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"time"
)

// Closed signal
type Closed struct{}

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

func (a ActionType) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

type LogMessage struct {
	ActionType ActionType `json:"action_type"`
	Detail     any        `json:"detail"`
}

func (msg LogMessage) String() string {
	if msg.Detail == nil {
		return msg.ActionType.String()
	}
	return fmt.Sprintf("%s: %v", msg.ActionType, msg.Detail)
}

// Worker
type Worker func(context.Context, types.Message) error

type ReceiveControllerConfig struct {
	// QueueURL (required)
	QueueURL string
	// ReceiveOption (required)
	ReceiveOption ReceiveOption
}

type ReceiveOption struct {
	// MaxNumberOfMessages (default: 1)
	MaxNumberOfMessages int32
	// VisibilityTimeout (default: 30s)
	VisibilityTimeout int32
	// WaitTimeSeconds (default: 20s)
	WaitTimeSeconds int32
	// DoChangeMessageVisibolity (default: false)
	DoChangeMessageVisibolity bool
	// ChangeMessageVisibilityAfter (default: 0)
	ChangeMessageVisibilityAfter time.Duration
}

func NewDefaultReceiveOption() ReceiveOption {
	return ReceiveOption{
		MaxNumberOfMessages:          1,
		VisibilityTimeout:            30,
		WaitTimeSeconds:              20,
		DoChangeMessageVisibolity:    false,
		ChangeMessageVisibilityAfter: 0,
	}
}

func (option *ReceiveOption) newReceiveMessageInput(queueUrl *string) *sqs.ReceiveMessageInput {
	return &sqs.ReceiveMessageInput{
		QueueUrl:            queueUrl,
		MaxNumberOfMessages: option.MaxNumberOfMessages,
		VisibilityTimeout:   option.VisibilityTimeout,
		WaitTimeSeconds:     option.WaitTimeSeconds,
	}
}

// ReceiveController
type ReceiveController interface {
	Run(context.Context, Worker) (<-chan Closed, <-chan Log)
}

type sqsClient interface {
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

type receiveController struct {
	client        sqsClient
	queueURL      string
	receiveOption ReceiveOption
}

func NewReceiveController(c *sqs.Client, cfg *ReceiveControllerConfig) ReceiveController {
	return &receiveController{
		client:        c,
		queueURL:      cfg.QueueURL,
		receiveOption: cfg.ReceiveOption,
	}
}

func (ctrl *receiveController) Run(ctx context.Context, work Worker) (<-chan Closed, <-chan Log) {
	closing := make(chan Closed, 0)
	log := make(chan Log, 10)

	go ctrl.run(ctx, work, closing, log)

	return closing, log
}

// run call from Run within goroutine.
func (ctrl *receiveController) run(ctx context.Context, work Worker, closing chan<- Closed, log chan<- Log) {
loop:
	for {
		select {
		case <-ctx.Done():
			close(closing)
			log <- &LogMessage{ActionType: ActionTypeClosed}

			return
		default:
			log <- &LogMessage{ActionType: ActionTypeReceivingMessage}

			result, err := ctrl.client.ReceiveMessage(ctx, ctrl.newReceiveMessageInput())
			if err != nil {
				log <- err
				continue loop
			}

			for _, m := range result.Messages {
				log <- &LogMessage{ActionType: ActionTypeReceivedMessage, Detail: *m.MessageId}

				ctrl.do(ctx, work, m, log)
			}
		}
	}
}

func (ctrl *receiveController) newReceiveMessageInput() *sqs.ReceiveMessageInput {
	return ctrl.receiveOption.newReceiveMessageInput(&ctrl.queueURL)
}

func (ctrl *receiveController) do(ctx context.Context, work Worker, m types.Message, log chan<- Log) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go ctrl.changeMessageVisibility(ctx, m, log)

	err := work(ctx, m)
	if err != nil {
		log <- err
		return
	}

	ctrl.deleteMessage(ctx, m, log)
}

func (ctrl *receiveController) deleteMessage(ctx context.Context, m types.Message, log chan<- Log) {
	log <- &LogMessage{ActionType: ActionTypeDeletingMessage, Detail: *m.ReceiptHandle}

	_, err := ctrl.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &ctrl.queueURL,
		ReceiptHandle: m.ReceiptHandle,
	})
	if err != nil {
		log <- err
		return
	}

	log <- &LogMessage{ActionType: ActionTypeDeletedMessage, Detail: *m.ReceiptHandle}
}

func (ctrl *receiveController) changeMessageVisibility(ctx context.Context, m types.Message, log chan<- Log) {
	if !ctrl.receiveOption.DoChangeMessageVisibolity {
		return
	}

	for {
		select {
		case <-time.After(ctrl.receiveOption.ChangeMessageVisibilityAfter):
			log <- &LogMessage{ActionType: ActionTypeChangingMessageVisibilityTimeout, Detail: *m.ReceiptHandle}

			_, err := ctrl.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl:      &ctrl.queueURL,
				ReceiptHandle: m.ReceiptHandle,
			})
			if err != nil {
				log <- err
				continue
			}

			log <- &LogMessage{ActionType: ActionTypeChangedMessageVisibilityTimeout, Detail: *m.ReceiptHandle}
		case <-ctx.Done():
			return
		}
	}
}
