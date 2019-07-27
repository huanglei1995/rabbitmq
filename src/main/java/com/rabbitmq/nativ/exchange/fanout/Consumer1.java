package com.rabbitmq.nativ.exchange.fanout;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Consumer1 {

    public static void main(String[] args) throws IOException, TimeoutException {
        // 创建连接工厂，并初始化ip,端口，用户名和密码
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("120.79.136.84");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        // 通过连接工厂创建连接
        Connection connection = connectionFactory.newConnection();

        // 通过连接创建一个信道
        final Channel channel = connection.createChannel();

        /**  定义交换机的类型和名称 , BuiltinExchangeType，交换机类型的枚举类
         */
        channel.exchangeDeclare(FanoutProduct.EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        // 声明一个随机队列
        String queueName = channel.queueDeclare().getQueue();

        String[] serverities = new String[]{"error", "info", "waring"};

        for (String serverity : serverities) {
            channel.queueBind(queueName, FanoutProduct.EXCHANGE_NAME, serverity);
        }
        System.out.println(" [*] Waiting for messages:");

        // 创建消费者
        final Consumer consumerA = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                System.out.println("Received:" + envelope.getRoutingKey() + "':'" + message + "'");
            }
        };

        channel.basicConsume(queueName, true, consumerA);
    }
}
