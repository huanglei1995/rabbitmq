package com.rabbitmq.nativ.produceconform;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 确认模式发送者，批量确认
 */
public class ProducerBatchConfirm {

    public static final String EXCHANGE_NAME = "producer_wait_confirm";
    public static final String ROUTER_KEY = "error";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
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
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        // 添加mandatory失败通知监听器
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body);
                System.out.println("RabbitMq返回的replyCode:  "+replyCode);
                System.out.println("RabbitMq返回的replyText:  "+replyText);
                System.out.println("RabbitMq返回的exchange:  "+exchange);
                System.out.println("RabbitMq返回的routingKey:  "+routingKey);
                System.out.println("RabbitMq返回的message:  "+message);
            }
        });
        // 启动发送者确认模式
        channel.confirmSelect();

        for (int i = 0; i < 2; i++) {
            String message = "Hello World_" + (i+1);
            channel.basicPublish(EXCHANGE_NAME, ROUTER_KEY, true, null, message.getBytes());
        }
        // 发送者确认，要么全部成功，要么全部死掉
        channel.waitForConfirmsOrDie();
        channel.close();
        connection.close();
    }
}
