package com.rabbitmq.nativ.getmessage;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class GetMessageConsumer {

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("120.79.136.84");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(GetMessageProduct.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = "focuserror";
        channel.queueDeclare(queueName,false,false, false, null);

        String routeKey = "error";
        channel.queueBind(queueName, GetMessageProduct.EXCHANGE_NAME, routeKey);

        System.out.println(" [*] Waiting for messages......");

        while (true) {
            GetResponse getResponse = channel.basicGet(queueName, true);
            if (null != getResponse) {
                System.out.println("received["
                        +getResponse.getEnvelope().getRoutingKey()+"]"
                        +new String(getResponse.getBody()));
            }
            Thread.sleep(1000);
        }
    }
}
