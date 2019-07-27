package com.rabbitmq.nativ.exchange.topic;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ServerAErrorConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        // 创建链接工厂
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("120.79.136.84");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");

        // 创建连接
        Connection connection = factory.newConnection();
        // 创建信道,指定交换机格式为topic
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(TopicProductor.EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, TopicProductor.EXCHANGE_NAME, "error.*.A");

        System.out.println("【*】waiting for message;");

        // 创建消费者
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body, "UTF-8");
                System.out.println("AllConsumer Received " + envelope.getRoutingKey() + "':" + msg + "'");
            }
        };
        channel.basicConsume(queueName,true, consumer);
    }
}
