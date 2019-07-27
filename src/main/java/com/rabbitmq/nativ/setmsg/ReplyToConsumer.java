package com.rabbitmq.nativ.setmsg;

import com.rabbitmq.client.*;
import com.rabbitmq.nativ.rejectmsg.RejectProducer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ReplyToConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("120.79.136.84");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        // 打开连接和创建频道，与发送端一样
        Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        channel.exchangeDeclare(ReplyToProducer.EXCHANGE_NAME,
                BuiltinExchangeType.DIRECT);

        String queueName = "focuserror";
        channel.queueDeclare(queueName, false, false,
                false, null);

        //只关注error级别的日志
        String routekey = "error";
        channel.queueBind(queueName, ReplyToProducer.EXCHANGE_NAME, routekey);

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
                System.out.println("Received [" + envelope.getRoutingKey() + "] " + message);
                AMQP.BasicProperties props = new AMQP.BasicProperties().builder()
                        .replyTo(properties.getReplyTo())
                        .correlationId(properties.getMessageId())
                        .build();
                channel.basicPublish("", props.getReplyTo(), props, ("Hi,"+message).getBytes("UTF-8"));
            }
        };
        channel.basicConsume(queueName, true, consumerB);
    }
}
