package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;


import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by lee on 5/16/17.
 */
public class DefaultPullConsumer implements PullConsumer{
    private KeyValue properties;
    private String queue;
    List<String> bucketList = new ArrayList<>();
    boolean isRegistered = false;
    int msgCnt = 0;



    private final int MQ_CAPACITY = 10000;
    BlockingQueue<Message> mq = null;
    MessageStation messageStation;




    public DefaultPullConsumer(KeyValue properties) {
        MessageStation.cntConsumer();
        this.properties = properties;
        mq = new LinkedBlockingQueue<>(MQ_CAPACITY);

        messageStation = MessageStation.getInstance(properties);

    }



    @Override public KeyValue properties() { return properties; }

    @Override public  Message poll() {
        if(!isRegistered)  {
            messageStation.register(this);
            isRegistered = true;
        }
        Message message = null;
        try {
            message = mq.take();
        } catch (InterruptedException e) { e.printStackTrace();}

        DefaultBytesMessage msg = (DefaultBytesMessage)message;

        if (new String(msg.getBody()).equals("")) {
            //System.out.println(Thread.currentThread().getName()+": "+msgCnt);
            return null;
        }


        msgCnt++;

        return  message;

    }








    @Override public Message poll(KeyValue properties) { throw new UnsupportedOperationException("Unsupported"); }

    @Override public  void attachQueue(String queueName, Collection<String> topics) {   //TODO:同步关键字可去
        if (queue != null && !queue.equals(queueName))
            throw new ClientOMSException("You have already attached to a queue: " + queue);
        queue = queueName;

        bucketList.addAll(topics);
        bucketList.add(queueName);

    }

    @Override public void ack(String messageId) { throw new UnsupportedOperationException("Unsupported"); }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

}
