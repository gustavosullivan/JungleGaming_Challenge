package sqs

import (
	"context"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/jungle-gaming/wagering/internal/app/port"
	"github.com/jungle-gaming/wagering/internal/platform/config"
)

type Client struct {
	api       *awssqs.Client
	queueURL  string
	eventsURL string
}

func New(cfg config.Config) (*Client, error) {
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion(cfg.SQSRegion),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AWSAccessKey, cfg.AWSSecretKey, "")),
	)
	if err != nil {
		return nil, err
	}
	api := awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
		o.BaseEndpoint = aws.String(cfg.SQSEndpoint)
	})
	return &Client{api: api, queueURL: cfg.QueueURL, eventsURL: cfg.EventsQueueURL}, nil
}

func (c *Client) Ready(ctx context.Context) error {
	if _, err := c.api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(c.queueURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	}); err != nil {
		return err
	}
	_, err := c.api.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
		QueueUrl: aws.String(c.eventsURL), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	return err
}

func (c *Client) Receive(ctx context.Context, max, wait, visibility int32) ([]port.Incoming, error) {
	out, err := c.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: max,
		WaitTimeSeconds:     wait,
		VisibilityTimeout:   visibility,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})
	if err != nil {
		return nil, err
	}
	msgs := make([]port.Incoming, 0, len(out.Messages))
	for _, m := range out.Messages {
		count := 1
		if raw, ok := m.Attributes["ApproximateReceiveCount"]; ok {
			if n, err := strconv.Atoi(raw); err == nil {
				count = n
			}
		}
		body := ""
		if m.Body != nil {
			body = *m.Body
		}
		receipt := ""
		if m.ReceiptHandle != nil {
			receipt = *m.ReceiptHandle
		}
		msgs = append(msgs, port.Incoming{Receipt: receipt, Body: []byte(body), ReceiveCount: count})
	}
	return msgs, nil
}

func (c *Client) Delete(ctx context.Context, receipt string) error {
	_, err := c.api.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
		QueueUrl: aws.String(c.queueURL), ReceiptHandle: aws.String(receipt),
	})
	return err
}

func (c *Client) ChangeVisibility(ctx context.Context, receipt string, timeout int32) error {
	_, err := c.api.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(c.queueURL), ReceiptHandle: aws.String(receipt), VisibilityTimeout: timeout,
	})
	return err
}

func (c *Client) ReceiveEvents(ctx context.Context, max int32) ([][]byte, error) {
	out, err := c.api.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.eventsURL),
		MaxNumberOfMessages: max,
		WaitTimeSeconds:     1,
		VisibilityTimeout:   5,
	})
	if err != nil {
		return nil, err
	}
	bodies := make([][]byte, 0, len(out.Messages))
	for _, m := range out.Messages {
		if m.Body != nil {
			bodies = append(bodies, []byte(*m.Body))
		}
		if m.ReceiptHandle != nil {
			_, _ = c.api.DeleteMessage(ctx, &awssqs.DeleteMessageInput{
				QueueUrl: aws.String(c.eventsURL), ReceiptHandle: m.ReceiptHandle,
			})
		}
	}
	return bodies, nil
}

func (c *Client) Publish(ctx context.Context, groupID, dedupID string, body []byte) error {
	_, err := c.api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(c.eventsURL),
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(groupID),
		MessageDeduplicationId: aws.String(dedupID),
	})
	return err
}

func (c *Client) SendInbound(ctx context.Context, groupID, dedupID string, body []byte) error {
	_, err := c.api.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(c.queueURL),
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(groupID),
		MessageDeduplicationId: aws.String(dedupID),
	})
	return err
}
