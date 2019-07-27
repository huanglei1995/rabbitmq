package com.rabbitmq.nativ.dlx;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class DlxProducer {

    public static final String EXCHANGE_NAME = "dlx_make";

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
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String[] serverities = {"error","info","warning"};
        for(int i=0;i<3;i++){
            String severity = serverities[i%3];
            String message = "Hello World_"+(i+1);
            //参数1：exchange name 参数2：routing key
            channel.basicPublish(EXCHANGE_NAME, severity,
                    null, message.getBytes());
            System.out.println(" [x] Sent 'error':'" + message + "'");
        }
        // 关闭频道和连接
        channel.close();
        connection.close();
    }
}
