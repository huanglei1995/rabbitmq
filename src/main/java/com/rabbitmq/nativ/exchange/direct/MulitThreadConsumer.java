package com.rabbitmq.nativ.exchange.direct;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 一个链接多个信道，一个队列多个消费者实现
 */
public class MulitThreadConsumer {

    private static class ConsumerWorker implements Runnable{

        final Connection connection;

        final String queueName;

        public ConsumerWorker(Connection connection, String queueName) {
            this.connection = connection;
            this.queueName = queueName;
        }

        @Override
        public void run() {
            try {
                // 创建一个信道，并定义交换机的名称和类型
                final Channel channel = connection.createChannel();
                channel.exchangeDeclare(DirectProduct.EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

                // 声明一个随机队列
                String factQueueName = queueName;
                final String consumerName;

                if (null == factQueueName) {
                    factQueueName = channel.queueDeclare().getQueue();
                    consumerName = Thread.currentThread().getName() + "-all";
                } else {
                    channel.queueDeclare(factQueueName, false, false, false, null);
                    consumerName = Thread.currentThread().getName();
                }

                // 所有的日志严重性级别
                String[] serverities = new String[]{"error", "info", "waring"};
                for (String routeKey : serverities) {
                    // 关注所有的日志级别
                    channel.queueBind(factQueueName, DirectProduct.EXCHANGE_NAME, routeKey); // 队列名称，交换机名称，路由键
                }
                // 绑定交换机
                System.out.println("【"+consumerName+"】waiting for message....");

                // 声明消费者
                final Consumer consumerA = new DefaultConsumer(channel){
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        String message = new String(body, "UTF-8");
                        System.out.println(consumerName + "Received["+ envelope.getRoutingKey() +"]:" + message);
                    }
                };
                // 消费者正式开始消费
                // 参数：队列名称，autoAck（true,就是消息被传递后就被确认）， 消费者
                channel.basicConsume(factQueueName, true, consumerA);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        // 创建连接工厂，并初始化ip,端口，用户名和密码
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("120.79.136.84");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        // 通过连接工厂创建连接
        Connection connection = connectionFactory.newConnection();

        /* 一个连接多个信道，每个连接都可以读取所有的数据 */
//        for (int i = 0; i < 2; i++) {
//            Thread worker = new Thread(new ConsumerWorker(connection, null));
//            worker.start();
//        }

        /* 一个队列多个消费者，每个消费者轮询消费 */
        String queueName = "focusAll";
        for (int i = 0; i < 2; i++) {
            Thread worker = new Thread(new ConsumerWorker(connection, queueName));
            worker.start();
        }
    }
}
