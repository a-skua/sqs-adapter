package main

import (
	"context"
	"errors"
	"github.com/a-skua/sqs-adapter"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	sqsClient   *sqs.Client
	queueURL    = os.Getenv("QUEUE_URL")
	sqsEndpoint = os.Getenv("SQS_ENDPOINT")
)

// mock
type endpointResolver struct{}

func (endpointResolver) ResolveEndpoint(string, sqs.EndpointResolverOptions) (aws.Endpoint, error) {
	return aws.Endpoint{
		URL: sqsEndpoint,
	}, nil
}

func init() {
	sqsClient = sqs.New(sqs.Options{
		EndpointResolver: endpointResolver{},
	})
}

func main() {
	// receiveOption := adapter.NewDefaultReceiveOption()
	receiveOption := adapter.ReceiveOption{
		MaxNumberOfMessages:          1,
		VisibilityTimeout:            10,
		WaitTimeSeconds:              20,
		DoChangeMessageVisibolity:    true,
		ChangeMessageVisibilityAfter: 7 * time.Second,
	}

	receive := adapter.NewReceiveController(sqsClient, &adapter.WorkerConfig{
		QueueURL:      queueURL,
		ReceiveOption: receiveOption,
	})

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, syscall.SIGINT, syscall.SIGTERM)
		<-sigint
		cancel()
	}()

	closed, logging := receive.Run(ctx, func(ctx context.Context, m types.Message) error {
		log.Println("---receive message---")
		log.Println("\tsleeping...10s")

		select {
		case <-time.After(10 * time.Second):
			log.Println("\tID  :", *m.MessageId)
			log.Println("\tBody:", *m.Body)
			return nil
		case <-ctx.Done():
			return errors.New("worker canceled")
		}
	})

	for {
		select {
		case l := <-logging:
			log.Println(l)
		case <-closed:
			time.Sleep(100 * time.Millisecond)
			for i := 0; i < len(logging); i++ {
				log.Println(<-logging)
			}
			return
		}
	}
}
