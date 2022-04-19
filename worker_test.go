package adapter

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"reflect"
	"testing"
	"time"
)

// TODO writing tests

type mockClient struct {
	// ReceiveMessage
	waitReceiveMessage     time.Duration
	countReceiveMessage    int
	responseReceiveMessage *sqs.ReceiveMessageOutput
	errorReceiveMessage    error
	// ChangeMessageVisibility
	waitChangeMessageVisibility     time.Duration
	countChangeMessageVisibility    int
	responseChangeMessageVisibility *sqs.ChangeMessageVisibilityOutput
	errorChangeMessageVisibility    error
	// DeleteMessage
	waitDeleteMessage     time.Duration
	countDeleteMessage    int
	responseDeleteMessage *sqs.DeleteMessageOutput
	errorDeleteMessage    error
}

type mockMessage struct {
	id            string
	receiptHandle string
}

func (m mockMessage) newMessage() types.Message {
	return types.Message{
		MessageId:     &m.id,
		ReceiptHandle: &m.receiptHandle,
	}
}

func (c1 *mockClient) Eq(c2 *mockClient) bool {
	return c1.countReceiveMessage == c2.countReceiveMessage &&
		c1.countChangeMessageVisibility == c2.countChangeMessageVisibility &&
		c1.countDeleteMessage == c2.countDeleteMessage
}

func (c *mockClient) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, ops ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	c.countReceiveMessage++

	select {
	case <-time.After(c.waitReceiveMessage):
		return c.responseReceiveMessage, c.errorReceiveMessage
	case <-ctx.Done():
		return c.responseReceiveMessage, errors.New("canceled")
	}
}

func (c *mockClient) DeleteMessage(ctx context.Context, in *sqs.DeleteMessageInput, ops ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	c.countDeleteMessage++

	select {
	case <-time.After(c.waitDeleteMessage):
		return c.responseDeleteMessage, c.errorDeleteMessage
	case <-ctx.Done():
		return c.responseDeleteMessage, errors.New("canceled")
	}
}

func (c *mockClient) ChangeMessageVisibility(ctx context.Context, in *sqs.ChangeMessageVisibilityInput, ops ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	c.countChangeMessageVisibility++

	select {
	case <-time.After(c.waitChangeMessageVisibility):
		return c.responseChangeMessageVisibility, c.errorChangeMessageVisibility
	case <-ctx.Done():
		return c.responseChangeMessageVisibility, errors.New("canceled")
	}
}

func TestNewDefaultReceiveOption(t *testing.T) {
	type test struct {
		testcase string
		want     ReceiveOption
	}

	do := func(tt test) {
		t.Run(tt.testcase, func(t *testing.T) {
			got := NewDefaultReceiveOption()
			want := tt.want
			if want != got {
				t.Fatalf("want=%v, got=%v.", want, got)
			}
		})
	}

	tests := []test{
		{
			testcase: "success",
			want: ReceiveOption{
				MaxNumberOfMessages:          1,
				VisibilityTimeout:            30,
				WaitTimeSeconds:              20,
				DoChangeMessageVisibolity:    false,
				ChangeMessageVisibilityAfter: 0,
			},
		},
	}

	for _, tt := range tests {
		do(tt)
	}
}

func TestReceiveOption_newReceiveMessageInput(t *testing.T) {
	type test struct {
		testcase string
		option   *ReceiveOption
		queueUrl *string
		want     *sqs.ReceiveMessageInput
	}

	do := func(tt test) {
		t.Run(tt.testcase, func(t *testing.T) {
			got := tt.option.newReceiveMessageInput(tt.queueUrl)
			want := tt.want

			if !reflect.DeepEqual(want, got) {
				t.Fatalf("want=%v, got=%v.", want, got)
			}
		})
	}

	tests := []test{
		func() test {
			queueURL := "http://localhost:9324/000000000000/queue"

			return test{
				testcase: "success",
				queueUrl: &queueURL,
				option: &ReceiveOption{
					MaxNumberOfMessages: 1,
					VisibilityTimeout:   30,
					WaitTimeSeconds:     20,
				},
				want: &sqs.ReceiveMessageInput{
					QueueUrl:            &queueURL,
					MaxNumberOfMessages: 1,
					VisibilityTimeout:   30,
					WaitTimeSeconds:     20,
				},
			}
		}(),
	}

	for _, tt := range tests {
		do(tt)
	}
}

func TestNewReceiveController(t *testing.T) {
	type test struct {
		testcase string
		client   *sqs.Client
		config   *ReceiveControllerConfig
		want     ReceiveController
	}

	do := func(tt test) {
		t.Run(tt.testcase, func(t *testing.T) {
			got := NewReceiveController(tt.client, tt.config)
			want := tt.want

			if !reflect.DeepEqual(want, got) {
				t.Fatalf("want=%v, got=%v.", want, got)
			}
		})
	}

	tests := []test{
		func() test {
			client := sqs.New(sqs.Options{})

			return test{
				testcase: "success",
				client:   client,
				config: &ReceiveControllerConfig{
					QueueURL:      "http://localhost:9324/000000000000/queue",
					ReceiveOption: NewDefaultReceiveOption(),
				},
				want: &receiveController{
					client:        client,
					queueURL:      "http://localhost:9324/000000000000/queue",
					receiveOption: NewDefaultReceiveOption(),
				},
			}
		}(),
	}

	for _, tt := range tests {
		do(tt)
	}
}

func TestReceiveController_Run(t *testing.T) {
	type test struct {
		testcase   string
		controller *receiveController // .client: *mockClient
		worker     Worker
		wait       time.Duration
		want       *mockClient
	}

	do := func(tt test) {
		t.Run(tt.testcase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			closed, log := tt.controller.Run(ctx, tt.worker)

			go func() {
				for {
					select {
					case l := <-log:
						t.Log(l)
					case _, ok := <-closed:
						if !ok {
							return
						}
					}
				}
			}()

			time.Sleep(tt.wait)
			cancel()

			got := tt.controller.client.(*mockClient)
			want := tt.want

			if !want.Eq(got) {
				t.Fatalf("want=%v, got=%v.", want, got)
			}

			_, ok := <-closed
			if ok {
				t.Fatal("failed closed channel")
			}
		})
	}

	tests := []test{
		{
			testcase: "received",
			controller: &receiveController{
				queueURL: "",
				client: &mockClient{
					waitReceiveMessage:     1 * time.Second,
					responseReceiveMessage: &sqs.ReceiveMessageOutput{},
				},
			},
			worker: func(context.Context, types.Message) error {
				return nil
			},
			wait: 100 * time.Millisecond,
			want: &mockClient{
				countReceiveMessage:          1,
				countDeleteMessage:           0,
				countChangeMessageVisibility: 0,
			},
		},
		{
			testcase: "error",
			controller: &receiveController{
				queueURL: "",
				client: &mockClient{
					waitReceiveMessage:     70 * time.Millisecond,
					countReceiveMessage:    0,
					responseReceiveMessage: &sqs.ReceiveMessageOutput{},
				},
			},
			worker: func(context.Context, types.Message) error {
				return errors.New("failed worker")
			},
			wait: 100 * time.Millisecond,
			want: &mockClient{
				countReceiveMessage:          2,
				countDeleteMessage:           0,
				countChangeMessageVisibility: 0,
			},
		},
		{
			testcase: "success empty",
			controller: &receiveController{
				queueURL: "",
				client: &mockClient{
					waitReceiveMessage:     70 * time.Millisecond,
					responseReceiveMessage: &sqs.ReceiveMessageOutput{},
					waitDeleteMessage:      1 * time.Millisecond,
				},
			},
			worker: func(context.Context, types.Message) error {
				return nil
			},
			wait: 100 * time.Millisecond,
			want: &mockClient{
				countReceiveMessage:          2,
				countDeleteMessage:           0,
				countChangeMessageVisibility: 0,
			},
		},
		{
			testcase: "success",
			controller: &receiveController{
				queueURL: "",
				client: &mockClient{
					waitReceiveMessage: 70 * time.Millisecond,
					responseReceiveMessage: &sqs.ReceiveMessageOutput{
						Messages: []types.Message{
							(mockMessage{"foo", "foo#bar"}).newMessage(),
							(mockMessage{"foo", "foo#bar"}).newMessage(),
							(mockMessage{"foo", "foo#bar"}).newMessage(),
						},
					},
				},
			},
			worker: func(context.Context, types.Message) error {
				return nil
			},
			wait: 100 * time.Millisecond,
			want: &mockClient{
				countReceiveMessage:          2,
				countDeleteMessage:           3,
				countChangeMessageVisibility: 0,
			},
		},
		{
			testcase: "success change-visibility",
			controller: &receiveController{
				queueURL: "",
				client: &mockClient{
					waitReceiveMessage: 1 * time.Millisecond,
					responseReceiveMessage: &sqs.ReceiveMessageOutput{
						Messages: []types.Message{
							(mockMessage{"foo", "foo#bar"}).newMessage(),
						},
					},
				},
				receiveOption: ReceiveOption{
					DoChangeMessageVisibolity:    true,
					ChangeMessageVisibilityAfter: 70 * time.Millisecond,
				},
			},
			worker: func(ctx context.Context, msg types.Message) error {
				select {
				case <-ctx.Done():
					return errors.New("canceled")
				case <-time.After(200 * time.Millisecond):
					return nil
				}
			},
			wait: 100 * time.Millisecond,
			want: &mockClient{
				countReceiveMessage:          1,
				countDeleteMessage:           0,
				countChangeMessageVisibility: 1,
			},
		},
		{
			testcase: "success with change-visibility",
			controller: &receiveController{
				queueURL: "",
				client: &mockClient{
					waitReceiveMessage: 30 * time.Millisecond,
					responseReceiveMessage: &sqs.ReceiveMessageOutput{
						Messages: []types.Message{
							(mockMessage{"foo", "foo#bar"}).newMessage(),
						},
					},
				},
				receiveOption: ReceiveOption{
					DoChangeMessageVisibolity:    true,
					ChangeMessageVisibilityAfter: 50 * time.Millisecond,
				},
			},
			worker: func(ctx context.Context, msg types.Message) error {
				select {
				case <-ctx.Done():
					return errors.New("canceled")
				case <-time.After(60 * time.Millisecond):
					return nil
				}
			},
			wait: 100 * time.Millisecond,
			want: &mockClient{
				countReceiveMessage:          2,
				countDeleteMessage:           1,
				countChangeMessageVisibility: 1,
			},
		},
	}

	for _, tt := range tests {
		do(tt)
	}
}
