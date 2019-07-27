package com.rabbitmq.exchange;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 绑定多个队列
 */
public class MulitBindConsumer {

    public static void main(String[] args) throws IOException, TimeoutException {
        // 创建连接工厂，并初始化ip,端口，用户名和密码
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("120.79.136.84");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        // 通过连接工厂创建连接
        Connection connection = connectionFactory.newConnection();
        // 创建一个信道，并定义交换机的名称和类型
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(DirectProduct.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        // 声明一个随机队列
        String queueName = channel.queueDeclare().getQueue();
        System.out.println(queueName);

        // 所有的日志严重性级别
        String[] serverities = new String[]{"error", "info", "waring", "debug"};
        for (String serverity : serverities) {
            // 绑定交换机
            String routeKey = serverity;
            channel.queueBind(queueName, DirectProduct.EXCHANGE_NAME, routeKey); // 队列名称，交换机名称，路由键
        }
        // 绑定交换机
        System.out.println("【*】waiting for message....");

        // 声明消费者
        final Consumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                super.handleDelivery(consumerTag, envelope, properties, body);
                String message = new String(body, "UTF-8");
                System.out.println("Received["+ envelope.getRoutingKey() +"]:" + message);
            }
        };

        // 消费者正式开始消费
        // 参数：队列名称，autoAck（true,就是消息被传递后就被确认）， 消费者
        channel.basicConsume(queueName, true, consumer);

    }
}
