package com.rabbitmq.nativ.mandatory;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ProductMandatory {

    public static final String EXCHANGE_NAME = "mandatory_logs";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        // 创建链接工厂
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("120.79.136.84");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        // 创建连接
        Connection connection = factory.newConnection();
        // 连接关闭时执行
        connection.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {
                System.out.println("");
            }
        });

        // 创建信道,指定交换机格式为topic
        final Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        String[] serveries =  {"info", "error", "waring"};


        // 添加信道关闭的监听
        channel.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {
                System.out.println("连接断开原因：" + cause.getMessage());
            }
        });

        // 添加mandatory失败通知监听器
        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode,
                                     String replyText,
                                     String exchange,
                                     String routingKey,
                                     AMQP.BasicProperties properties,
                                     byte[] body) throws IOException {
                String message = new String(body);
                System.out.println("返回的replyText文本:" + replyText);
                System.out.println("返回的exchange:" + exchange);
                System.out.println("返回的routingKey:" + routingKey);
                System.out.println("返回的body:" + message);
            }
        });
        for (String servery : serveries) {
            String message = "Hello World_" + servery;
            // 第三个参数代表启动失败通知
            channel.basicPublish(EXCHANGE_NAME, servery, true, null, message.getBytes());
            System.out.println("----------------------------------");
            System.out.println("Send Message:[" + servery + "]:'" + message + "'");
            Thread.sleep(200);
        }

        channel.close();
        connection.close();
    }
}
