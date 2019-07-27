package com.rabbitmq.nativ.getmessage;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class GetMessageProduct {
    public final static String EXCHANGE_NAME = "direct_logs";


    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("120.79.136.84");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

        for (int i = 0; i < 3; i++) {
            String message = "Hello World_" + (i+1);
            channel.basicPublish(EXCHANGE_NAME, "error", null, message.getBytes());
            System.out.println(" [x] Sent 'error':'"
                    + message + "'");
        }

        channel.close();
        connection.close();
    }
}
