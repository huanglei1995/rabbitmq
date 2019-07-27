package com.rabbitmq.nativ.transaction;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ProducerTransaction {

    public final static String EXCHANGE_NAME = "producer_transaction";

    public static void main(String[] args) throws IOException, TimeoutException{
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

        String[] serveries =  {"info", "error", "waring"};
        // 开始事物
        channel.txSelect();
        try {
            for (String servery : serveries) {
                String message = "Hello World_" + servery;
                // 第三个参数代表启动失败通知
                channel.basicPublish(EXCHANGE_NAME, servery, true, null, message.getBytes());
                System.out.println("----------------------------------");
                System.out.println("Send Message:[" + servery + "]:'" + message + "'");
                Thread.sleep(200);
            }
            // 提交事务
            channel.txCommit();
        } catch (InterruptedException e) {
            // 事务回滚
            channel.txRollback();
            e.printStackTrace();
        } catch (IOException e) {
            channel.txRollback();
            e.printStackTrace();
        }

        channel.close();
        connection.close();
    }
}
