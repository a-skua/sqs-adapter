package adapter

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"time"
)

type Closed struct{}
type Log any

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

	go ctrl.runner(ctx, work, closing, log)

	return closing, log
}

// runner call from Run within goroutine.
func (ctrl *receiveController) runner(ctx context.Context, work Worker, closing chan<- Closed, log chan<- Log) {
	for {
		select {
		case <-ctx.Done():
			log <- "closed"
			close(closing)
			return
		default:
			log <- "receive message"
			result, err := ctrl.client.ReceiveMessage(ctx, ctrl.newReceiveMessageInput())
			if err != nil {
				log <- err
				break
			}

			for _, m := range result.Messages {
				ctrl.receive(ctx, work, m, log)
			}
		}
	}
}

func (ctrl *receiveController) newReceiveMessageInput() *sqs.ReceiveMessageInput {
	return ctrl.receiveOption.newReceiveMessageInput(&ctrl.queueURL)
}

func (ctrl *receiveController) receive(ctx context.Context, work Worker, m types.Message, log chan<- Log) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go ctrl.changeMessageVisibility(ctx, m, log)

	err := work(ctx, m)
	if err != nil {
		log <- err
		return
	}

	log <- "delete message"
	_, err = ctrl.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &ctrl.queueURL,
		ReceiptHandle: m.ReceiptHandle,
	})
	if err != nil {
		log <- err
	}
}

func (ctrl *receiveController) changeMessageVisibility(ctx context.Context, m types.Message, log chan<- Log) {
	if !ctrl.receiveOption.DoChangeMessageVisibolity {
		return
	}

	for {
		select {
		case <-time.After(ctrl.receiveOption.ChangeMessageVisibilityAfter):
			log <- "change message visibility"
			_, err := ctrl.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
				QueueUrl:      &ctrl.queueURL,
				ReceiptHandle: m.ReceiptHandle,
			})
			if err != nil {
				log <- err
			}
		case <-ctx.Done():
			return
		}
	}
}
