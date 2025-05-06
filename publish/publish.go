package publish

import "context"

type Publisher interface {
	Publish(ctx context.Context, topic string, data string) error
}
