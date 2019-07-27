package com.rabbitmq.nativ.dlx;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class DlxProcessConsumer {
    public final static String DLX_EXCHANGE_NAME = "dlx_accept";

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
        channel.exchangeDeclare(DLX_EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // 声明一个队列,并绑定死信交换器
        String queueName = "dlx_accept";
        channel.queueDeclare(queueName, false, false, false, null);

        // 绑定
        channel.queueBind(queueName, DLX_EXCHANGE_NAME, "#");
        System.out.println("waiting for message........");

        // 声明也给消费者,单次qos
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received["
                        +envelope.getRoutingKey()
                        +"]"+message);
            }
        };
        // 消费者开始在队列中消费
        channel.basicConsume(queueName, true, consumer);
    }
}
