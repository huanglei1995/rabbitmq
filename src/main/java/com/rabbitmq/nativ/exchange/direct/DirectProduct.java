package com.rabbitmq.nativ.exchange.direct;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 存放到延迟队列的元素，对业务数据进行了包装
 */
public class DirectProduct {

    public static final String EXCHANGE_NAME = "direct_log";

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
        Channel channel = connection.createChannel();

        /**
         * 定义交换机的类型和名称
         * BuiltinExchangeType，交换机类型的枚举类
         */
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);


        String[] serverities = new String[]{"error", "info", "waring", "debug"};

        for (String serverity : serverities) {
            String msg = "Hello, RabbitMQ:" + (serverity);

            channel.basicPublish(EXCHANGE_NAME, serverity, null, msg.getBytes());
            System.out.println("send message: " + msg);
        }

        channel.close();
        connection.close();

    }
}
