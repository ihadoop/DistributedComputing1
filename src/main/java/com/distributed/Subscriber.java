package com.distributed;

import org.zeromq.ZMQ;

public class Subscriber {

    private final String topic;
    int port;
    ZMQ.Context context;
    ZMQ.Socket subscriber;
    public Subscriber(String topic,int port) {
        this.topic = topic;
        this.port = port;
         context = ZMQ.context(1);
         subscriber = context.socket(ZMQ.SUB);  // Create the SUB socket
        subscriber.connect("tcp://localhost:"+port);  // Connect to the publisher

        subscriber.subscribe(topic);  // Subscribe to the specified topic
    }

    public static void main(String[] args) {
        Subscriber subscriber = new Subscriber("hello",5555);  // Subscribe to "worker1"
        subscriber.run();
    }

    public byte [] run() {
                // Receive messages matching the topic
                String topicReceived = subscriber.recvStr(0);  // Receive the topic
                byte [] message = subscriber.recv(0);  // Receive the actual message
                System.out.println("Received: Topic: " + topicReceived + " Message: " + message);
return message;
    }
}
