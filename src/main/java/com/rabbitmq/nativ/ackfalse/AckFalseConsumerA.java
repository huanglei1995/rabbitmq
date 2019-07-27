package com.rabbitmq.nativ.ackfalse;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.TimeoutException;

public class AckFalseConsumerA {

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
        channel.exchangeDeclare(AckFalseProducter.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        // 声明一个队列
        String queuName = "focuserror";
        channel.queueDeclare(queuName,false, false, false, null);

        // 将队列，交换机，路由进行绑定
        channel.queueBind(queuName, AckFalseProducter.EXCHANGE_NAME, "error");

        System.out.println("waiting for message........");

        // 声明消费者
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    String message = new String(body, "UTF-8");
                    System.out.println("Received["+envelope.getRoutingKey()
                            +"]"+message);
                    //确认
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                    //拒绝
                }
            }
        };
        // 消费者开始在指定队列进行消费
        channel.basicConsume(queuName, false, consumer);
    }
}
