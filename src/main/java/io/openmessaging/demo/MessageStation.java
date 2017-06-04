package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by lee on 6/4/17.
 */
public class MessageStation {
    private KeyValue properties;

    List<DefaultPullConsumer> consumerList;
    List<String> producerList;

    static  AtomicInteger numOfConsumer = new AtomicInteger(0);

    private static volatile  MessageStation INSTANCE = null;


    private MessageStation(KeyValue properties) {
        this.properties = properties;
        consumerList = new ArrayList<>();
        producerList = new ArrayList<>();

        getFileSet();
    }

    public void loadFile() {
        for (String producerId: producerList) {
            Thread thr = new Thread(new MessageReader(properties, producerId, consumerList));
            thr.start();
            try {
                thr.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        sendFinishFlag();

    }

    public static void cntConsumer() {
        numOfConsumer.incrementAndGet();
        //System.out.println("numOfConsumer: " + numOfConsumer.get());
    }


    public static MessageStation getInstance(KeyValue properties) {
        if (INSTANCE == null) {
            synchronized (MessageStation.class) {
                if (INSTANCE == null) {
                    INSTANCE = new MessageStation(properties);
                }
            }
        }
        return INSTANCE;
    }

    public void getFileSet() {
        String absPath = properties.getString("STORE_PATH");
        File dir = new File(absPath);
        File[] producerFiles = null;

        producerFiles = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("Thread-");
            }
        });
        for(File producerFile: producerFiles)
            producerList.add(producerFile.getName());
    }

    public void register(DefaultPullConsumer consumer) {
        synchronized (consumerList) {
            consumerList.add(consumer);
        }
        if (consumerList.size() == numOfConsumer.get()) //  注册完后，启动读线程
            loadFile();




    }

    public void sendFinishFlag() {
        DefaultMessageFactory messageFactory = new DefaultMessageFactory();
        Message message = messageFactory.createBytesMessageToQueue("", "".getBytes());
        for (DefaultPullConsumer consumer: consumerList) {
            try {
                consumer.mq.put(message);
            } catch (InterruptedException e) { e.printStackTrace();}
        }
    }








}
