package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/thanhkaiba/wrap-amqp/amqpwrapper"
	"time"
)

func main() {
	config := amqpwrapper.Config{
		Connection: amqpwrapper.ConnectionConfig{
			AmqpURI: "amqp://daniel:daniel@127.0.0.1:5672",
			AmqpConfig: &amqp.Config{
				ChannelMax: 100,
			},
			Reconnect: amqpwrapper.DefaultReconnectConfig(),
		},
		Publish: amqpwrapper.PublishConfig{
			Mandatory:       false,
			Immediate:       false,
			ChannelPoolSize: 1,
			ConfirmDelivery: false,
		},
	}
	p, err := amqpwrapper.NewPublisher(config, amqpwrapper.DefaultLogger{})
	if err != nil {
		fmt.Println(err)
		return
	}
	time.Sleep(1 * time.Minute)
	p.Close()

	time.Sleep(100 * time.Hour)
}
