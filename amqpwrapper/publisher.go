package amqpwrapper

import (
	"fmt"
	"github.com/hashicorp/go-multierror"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"

	"github.com/pkg/errors"
)

type Publisher struct {
	*ConnectionWrapper

	config                  Config
	publishBindingsLock     sync.RWMutex
	publishBindingsPrepared map[string]struct{}
	closePublisher          func() error
	chanProvider            channelProvider
}

func NewPublisher(config Config, logger LoggerAdapter) (*Publisher, error) {
	if err := config.ValidatePublisher(); err != nil {
		return nil, err
	}

	var err error

	conn, err := NewConnection(config.Connection, logger)
	if err != nil {
		return nil, fmt.Errorf("create new connection: %w", err)
	}

	chanProvider, err := newChannelProvider(conn, config.Publish.ChannelPoolSize, config.Publish.ConfirmDelivery, logger)
	if err != nil {
		return nil, fmt.Errorf("create new channel pool: %w", err)
	}

	// Close the connection when the publisher is closed since this publisher owns the connection.
	closePublisher := func() error {
		logger.Debug("Closing publisher connection.", nil)

		chanProvider.Close()

		return conn.Close()
	}

	return &Publisher{
		conn,
		config,
		sync.RWMutex{},
		make(map[string]struct{}),
		closePublisher,
		chanProvider,
	}, nil
}

// Publish publishes messages to AMQP broker.
// Publish is blocking until the broker has received and saved the message.
// Publish is always thread safe.
//
// Watermill's topic in Publish is not mapped to AMQP's topic, but depending on configuration it can be mapped
// to exchange, queue or routing key.
// For detailed description of nomenclature mapping, please check "Nomenclature" paragraph in doc.go file.
func (p *Publisher) Publish(queue string, data []byte) (err error) {
	if p.Closed() {
		return errors.New("AMQP is connection closedChan")
	}

	if !p.IsConnected() {
		return errors.New("not connected to AMQP")
	}

	p.connectionWaitGroup.Add(1)
	defer p.connectionWaitGroup.Done()

	c, err := p.chanProvider.Channel()
	if err != nil {
		return errors.Wrap(err, "cannot open channel")
	}
	defer func() {
		if channelCloseErr := p.chanProvider.CloseChannel(c); channelCloseErr != nil {
			err = multierror.Append(err, channelCloseErr)
		}
	}()

	channel := c.AMQPChannel()

	var q amqp.Queue
	q, err = channel.QueueDeclare(queue, false, false, false, false, nil)
	if err != nil {
		return err
	}
	return p.publishMessage("", q.Name, data, c)
}

func (p *Publisher) Close() error {
	return p.closePublisher()
}

func (p *Publisher) publishMessage(
	exchangeName, routingKey string,
	msg []byte,
	channel channel,
) error {
	var err error
	if err != nil {
		return errors.Wrap(err, "cannot marshal message")
	}

	amqpMsg := amqp.Publishing{
		ContentType: "application/octet-stream",
		Body:        msg,
	}
	if err = channel.AMQPChannel().Publish(
		exchangeName,
		routingKey,
		p.config.Publish.Mandatory,
		p.config.Publish.Immediate,
		amqpMsg,
	); err != nil {
		return errors.Wrap(err, "cannot publish msg")
	}

	return nil
}
