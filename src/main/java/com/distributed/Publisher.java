package com.distributed;

import org.zeromq.ZMQ;

public class Publisher {

    ZMQ.Context context;
    ZMQ.Socket publisher;

    public Publisher(int port) {
        context = ZMQ.context(1);
        publisher = context.socket(ZMQ.PUB);  // Create the PUB socket
        publisher.bind("tcp://*:" + port);  // Bind to a port
    }

    public void send(String topic, byte[] data) {
        publisher.sendMore(topic);  // Topic: worker + id
        publisher.send(data);  // Message body
    }


    public static void main(String[] args) {
        Publisher publisher = new Publisher(5555);

        try {
            Thread.sleep(10000) ;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        publisher.send("hello", "world".getBytes());

        try {
            Thread.sleep(10000) ;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
