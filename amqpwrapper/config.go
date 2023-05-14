package amqpwrapper

import (
	"crypto/tls"
	"github.com/cenkalti/backoff/v3"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type Config struct {
	Connection ConnectionConfig
	Publish    PublishConfig
}

func (c Config) validate(validateConnection bool) error {
	var err error

	if validateConnection {
		if c.Connection.AmqpURI == "" {
			err = multierror.Append(err, errors.New("empty Config.AmqpURI"))
		}
	}

	return err
}

func (c Config) validatePublisher(validateConnection bool) error {
	err := c.validate(validateConnection)
	return err
}

func (c Config) ValidatePublisher() error {
	return c.validatePublisher(true)
}

type ConnectionConfig struct {
	AmqpURI string

	TLSConfig  *tls.Config
	AmqpConfig *amqp.Config

	Reconnect *ReconnectConfig
}

type PublishConfig struct {

	// Publishings can be undeliverable when the mandatory flag is true and no queue is
	// bound that matches the routing key, or when the immediate flag is true and no
	// consumer on the matched queue is ready to accept the delivery.
	Mandatory bool

	// Publishings can be undeliverable when the mandatory flag is true and no queue is
	// bound that matches the routing key, or when the immediate flag is true and no
	// consumer on the matched queue is ready to accept the delivery.
	Immediate bool

	// ChannelPoolSize specifies the size of a channel pool. All channels in the pool are opened when the publisher is
	// created. When a Publish operation is performed then a channel is taken from the pool to perform the operation and
	// then returned to the pool once the operation has finished. If all channels are in use then the Publish operation
	// waits until a channel is returned to the pool.
	// If this value is set to 0 (default) then channels are not pooled and a new channel is opened/closed for every
	// Publish operation.
	ChannelPoolSize int

	// ConfirmDelivery indicates whether the Publish function should wait until a confirmation is received from
	// the AMQP server in order to guarantee that the message is delivered. Setting this value to true may
	// negatively impact performance but will increase reliability.
	ConfirmDelivery bool
}

type ReconnectConfig struct {
	BackoffInitialInterval     time.Duration
	BackoffRandomizationFactor float64
	BackoffMultiplier          float64
	BackoffMaxInterval         time.Duration
}

func DefaultReconnectConfig() *ReconnectConfig {
	return &ReconnectConfig{
		BackoffInitialInterval:     500 * time.Millisecond,
		BackoffRandomizationFactor: 0.5,
		BackoffMultiplier:          1.5,
		BackoffMaxInterval:         60 * time.Second,
	}
}

func (r ReconnectConfig) backoffConfig() *backoff.ExponentialBackOff {
	return &backoff.ExponentialBackOff{
		InitialInterval:     r.BackoffInitialInterval,
		RandomizationFactor: r.BackoffRandomizationFactor,
		Multiplier:          r.BackoffMultiplier,
		MaxInterval:         r.BackoffMaxInterval,
		MaxElapsedTime:      0, // no support for disabling reconnect, only close of AMQP can stop reconnecting
		Clock:               backoff.SystemClock,
	}
}
