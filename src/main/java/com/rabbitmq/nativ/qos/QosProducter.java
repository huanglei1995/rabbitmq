package com.rabbitmq.nativ.qos;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class QosProducter{
    public final static String EXCHANGE_NAME = "direct_logs";

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
        // 发送210条消息，其中第210条消息表示本批次消息的结束
        for(int i=0;i<210;i++){
            String message = "Hello World_"+(i+1);
            if(i==209){
                message = "stop";
            }
            //参数1：exchange name 参数2：routing key
            channel.basicPublish(EXCHANGE_NAME, "error",
                    null, message.getBytes());
            System.out.println(" [x] Sent 'error':'"
                    + message + "'");
        }
        // 关闭频道和连接
        channel.close();
        connection.close();
    }
}
