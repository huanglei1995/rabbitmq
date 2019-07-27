package com.rabbitmq.nativ.exchange.fanout;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * fanout交换机模式，不会理会routerkey，会忽略他，获取所有的数据
 */
public class FanoutProduct {
    public static final String EXCHANGE_NAME = "fanout_logs";

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

        /**
         * fanout指定转发
         * 定义交换机的类型和名称 , BuiltinExchangeType，交换机类型的枚举类*/
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT);

        String queueName = "producer_create";
        channel.queueDeclare(queueName, false,false, false, null);
        channel.queueBind(queueName, EXCHANGE_NAME, "test");

        String[] serverities = new String[]{"error", "info", "waring"};

        for (String serverity : serverities) {
            String msg = "Hello, RabbitMQ:" + (serverity);

            channel.basicPublish(EXCHANGE_NAME, serverity, null, msg.getBytes());
            System.out.println("send message: " + msg);
        }

        channel.close();
        connection.close();
    }

}
