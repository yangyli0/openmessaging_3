package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;

/**
 * Created by lee on 6/4/17.
 */
public class MessageReader implements  Runnable{
    KeyValue properties;

    private final  int BUFFER_SIZE = 512 * 1024 * 1024;
    String fileName;
    FileChannel fc;
    MappedByteBuffer mapBuf;


    List<DefaultPullConsumer> consumerList;

    public MessageReader(KeyValue properties, String fileName, List<DefaultPullConsumer> consumerList) {
        this.properties = properties;
        this.fileName = fileName;
        this.consumerList = consumerList;
    }
    public void run() {
        String absPath = properties.getString("STORE_PATH") + "/" + fileName;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(absPath, "r");
            fc = raf.getChannel();
            mapBuf = fc.map(FileChannel.MapMode.READ_ONLY, 0, BUFFER_SIZE);
        } catch (IOException e) { e.printStackTrace();}

        while (true) {
            int i = mapBuf.position();
            if (i < mapBuf.capacity() && mapBuf.get(i) == 0)   {
                break;  //文件已经读完
            }
            for(; i < mapBuf.capacity() && mapBuf.get(i) != 44; i++);

            int len = i - mapBuf.position();
            byte[] lenBytes = new byte[len];
            mapBuf.get(lenBytes, 0, len);
            mapBuf.get();   //跳过','
            int msgLen = Integer.parseInt(new String(lenBytes));

            byte[] msgBytes = new byte[msgLen];

            mapBuf.get(msgBytes, 0, msgLen);
            Message message = assemble(msgBytes);
            dispatch(message);

        }

    }

    public void dispatch(Message message) {
        String bucket;
        if (message.headers().containsKey(MessageHeader.QUEUE))
            bucket = message.headers().getString(MessageHeader.QUEUE);
        else
            bucket = message.headers().getString(MessageHeader.TOPIC);
        synchronized (consumerList) {
            for (DefaultPullConsumer consumer: consumerList) {
                if (consumer.bucketList.contains(bucket)) {
                    try {
                        consumer.mq.put(message);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }




    public Message assemble(byte[] msgBytes) {
        int i, j;
        // 获取property

        DefaultKeyValue property = new DefaultKeyValue();
        for (i = 0; i < msgBytes.length && msgBytes[i] != ','; i++) ;
        byte[] propertyBytes = Arrays.copyOfRange(msgBytes, 0, i);  // [start, end)
        insertKVs(propertyBytes, property);
        j = ++i; // 跳过","

        // 获取headers
        DefaultKeyValue header = new DefaultKeyValue();
        for (; i < msgBytes.length && msgBytes[i] != ','; i++) ;
        byte[] headerBytes = Arrays.copyOfRange(msgBytes, j, i);
        insertKVs(headerBytes, header);
        j = ++i; // 跳过","

        // 获取body
        for (; i < msgBytes.length && msgBytes[i] != '\n'; i++) ;
        byte[] body = Arrays.copyOfRange(msgBytes, j, i);

        String queueOrTopic = header.getString(MessageHeader.TOPIC);
        DefaultBytesMessage message = null;
        DefaultMessageFactory messageFactory = new DefaultMessageFactory();
        if (queueOrTopic != null)
            message = (DefaultBytesMessage) messageFactory.createBytesMessageToTopic(queueOrTopic, body);
        else
            message = (DefaultBytesMessage) messageFactory.createBytesMessageToQueue(queueOrTopic, body);

        message.setHeaders(header);
        message.setProperties(property);

        return message;
    }

    public void insertKVs(byte[] kvBytes, KeyValue map) {
        String kvStr = new String(kvBytes);
        String[] kvPairs = kvStr.split("\\|");
        for (String kv: kvPairs) {

            String[] tuple = kv.split("#");

            if(tuple[1].startsWith("i"))
                map.put(tuple[0], Integer.parseInt(tuple[1].substring(1)));
            else if(tuple[1].startsWith("d"))
                map.put(tuple[0], Double.parseDouble(tuple[1].substring(1)));
            else if (tuple[1].startsWith("l"))
                map.put(tuple[0], Long.parseLong(tuple[1].substring(1)));
            else
                map.put(tuple[0], tuple[1].substring(1));

        }

    }

}

















