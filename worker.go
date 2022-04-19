package adapter

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"time"
)

// Closed signal
type Closed struct{}

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

// receiveController implements ReceiveController
type receiveController struct {
	client   sqsClient
	queueURL string
	option   ReceiveOption
}

func NewReceiveController(c *sqs.Client, cfg *ReceiveControllerConfig) ReceiveController {
	return &receiveController{
		client:   c,
		queueURL: cfg.QueueURL,
		option:   cfg.ReceiveOption,
	}
}

func (ctrl *receiveController) Run(ctx context.Context, work Worker) (<-chan Closed, <-chan Log) {
	closing := make(chan Closed, 0)
	log := make(chan Log, 10)

	go ctrl.run(ctx, work, closing, log)

	return closing, log
}

func (ctrl *receiveController) newReceiveMessageInput() *sqs.ReceiveMessageInput {
	return ctrl.option.newReceiveMessageInput(&ctrl.queueURL)
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
	if !ctrl.option.DoChangeMessageVisibolity {
		return
	}

	for {
		select {
		case <-time.After(ctrl.option.ChangeMessageVisibilityAfter):
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
