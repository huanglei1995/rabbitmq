package com.rabbitmq.nativ.produceconform;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 确认模式发送者，异步操作
 */
public class ProducerConfirmAsync {

    public static final String EXCHANGE_NAME = "producer_async_confirm";
    public static final String ROUTER_KEY = "error";

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
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        // 启动发送者确认模式
        channel.confirmSelect();

        // 启动发送者确认监听
        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("deliveryTag:"+deliveryTag
                        +",multiple:"+multiple);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {

            }
        });

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

        String[] severities={"error","warning"};
        for (int i = 0; i < 100; i++) {
            String severity = severities[i%2];
            // 发送的消息
            String message = "Hello World_"+(i+1)+("_"+System.currentTimeMillis());
            channel.basicPublish(EXCHANGE_NAME, severity, true,
                    MessageProperties.PERSISTENT_BASIC, message.getBytes());
            System.out.println("----------------------------------------------------");
            System.out.println(" Sent Message: [" + severity +"]:'"+ message + "'");
        }
//
//        channel.close();
//        connection.close();
    }
}
