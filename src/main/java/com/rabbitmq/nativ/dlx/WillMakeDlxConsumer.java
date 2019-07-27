package com.rabbitmq.nativ.dlx;

import com.rabbitmq.client.*;
import com.rabbitmq.nativ.qos.BatchAckConsumer;
import com.rabbitmq.nativ.qos.QosProducter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

// 普通消费者
public class WillMakeDlxConsumer {

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
        channel.exchangeDeclare(DlxProducer.EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        // 声明一个队列,并绑定死信交换器
        String queueName = "dlx_make";
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("x-dead-letter-exchange", DlxProcessConsumer.DLX_EXCHANGE_NAME);
        channel.queueDeclare(queueName, false, false, false, arguments);

        // 绑定
        channel.queueBind(queueName, DlxProducer.EXCHANGE_NAME, "#");
        System.out.println("waiting for message........");

        // 声明也给消费者,单次qos
        final Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                String message = new String(body, "UTF-8");
                if(envelope.getRoutingKey().equals("error")){
                    System.out.println("Received["
                            +envelope.getRoutingKey()
                            +"]"+message);
                    channel.basicAck(envelope.getDeliveryTag(),
                            false);
                }else{
                    System.out.println("Will reject["
                            +envelope.getRoutingKey()
                            +"]"+message);
                    channel.basicReject(envelope.getDeliveryTag(),
                            false);
                }
            }
        };
        // 消费者开始在队列中消费
        channel.basicConsume(queueName, false, consumer);
    }
}
