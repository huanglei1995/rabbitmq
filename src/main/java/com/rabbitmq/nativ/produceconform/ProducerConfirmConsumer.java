package com.rabbitmq.nativ.produceconform;

import com.rabbitmq.client.*;
import com.rabbitmq.nativ.transaction.ProducerTransaction;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ProducerConfirmConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("120.79.136.84");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        // 打开连接和创建频道，与发送端一样
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.exchangeDeclare(ProducerConfirmAsync.EXCHANGE_NAME,
                BuiltinExchangeType.DIRECT);

        String queueName = ProducerConfirmAsync.EXCHANGE_NAME;
        channel.queueDeclare(queueName,false,false,
                false,null);

        //只关注error级别的日志
        String severity="error";
        channel.queueBind(queueName, ProducerConfirmAsync.EXCHANGE_NAME, severity);

        System.out.println(" [*] Waiting for messages......");

        // 创建队列消费者
        final Consumer consumerB = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag,
                                       Envelope envelope,
                                       AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                //记录日志到文件：
                System.out.println( "Received ["+ envelope.getRoutingKey()
                        + "] "+message);
            }
        };
        channel.basicConsume(queueName, true, consumerB);
    }
}
