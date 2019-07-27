package com.rabbitmq.nativ.setmsg;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

public class ReplyToProducer {

    public final static String EXCHANGE_NAME = "replyto";

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
        final Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, false);

        String responseQueue = channel.queueDeclare().getQueue();
        String msgId = UUID.randomUUID().toString();
        AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                .replyTo(responseQueue)
                .messageId(msgId)
                .build();

        // 声明一个消费者，用来获取消费方返回的数据
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String msg = new String(body, "UTF-8");
                System.out.println("Received["+envelope.getRoutingKey()
                        +"]"+msg);
            }
        };
        // 消费方开始消费数据
        channel.basicConsume(responseQueue, true, consumer);

        String message = "Hello World,RabbitMQ";
        channel.basicPublish(EXCHANGE_NAME, "error", props, message.getBytes());
        System.out.println("Sent error:"+message);
    }
}
