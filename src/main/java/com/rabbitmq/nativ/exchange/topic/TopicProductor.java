package com.rabbitmq.nativ.exchange.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TopicProductor {
    public static final String EXCHANGE_NAME = "topic_logs";

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
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String[] serveries =  {"info", "error", "waring"};
        String[] modules = {"user", "order", "email"};
        String[] servers = {"A", "B", "C"};
        for (int i = 0; i < serveries.length; i++) {
            for (int i1 = 0; i1 < modules.length; i1++) {
                for (int i2 = 0; i2 < servers.length; i2++) {
                    String message = "Hello Topic_【" + i + "," + i1 + "," + i2 +"】";
                    String routeKey = serveries[i % 3]+ "." + modules[i1%3] + "." + servers[i2%3];
                    channel.basicPublish(EXCHANGE_NAME, routeKey, null, message.getBytes());
                }
            }
        }

        // 关闭连接和信道
        channel.close();
        connection.close();

    }

}
