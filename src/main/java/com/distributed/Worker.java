package com.distributed;

import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.*;

public class Worker {

    private final int workerId;
    private int lamportClock = 0;  // Lamport clock for this worker
    private int times = 0;

    public Worker(int workerId) {
        this.workerId = workerId;
    }

    public static void main(String[] args) {

        final int workerId = 4;  // Worker ID passed as a command-line argument
        Worker worker = new Worker(workerId);
        worker.run();

    }

    public void run() {
        Subscriber subscriber = new Subscriber(workerId + "", 5555);
        Publisher publisher = new Publisher(8888+workerId);

        while (true) {

            byte[] message = subscriber.run();
            ByteBuffer buffer = ByteBuffer.wrap(message);
            int receivedClock = buffer.getInt();
            lamportClock = Math.max(lamportClock, receivedClock) + 1;

            byte[] chunk = new byte[buffer.remaining()];
            buffer.get(chunk);  // Get the chunk of data

            System.out.println("Receive Message");
            publisher.send(workerId+"",serializeChunk(chunk,lamportClock));
        }
    }

    // Serialize the chunk and attach the Lamport clock value
    private byte[] serializeChunk(byte[] chunk, int lamportClock) {
        ByteBuffer buffer = ByteBuffer.allocate(chunk.length + Integer.BYTES);
        buffer.putInt(lamportClock);  // Add Lamport clock (4 bytes)
        buffer.put(chunk);  // Add the chunk
        return buffer.array();
    }

    // Process the chunk of data (for now, we convert it to uppercase)
    private String processChunk(byte[] chunk) {
        // For example: just convert the bytes to uppercase
        String processed = new String(chunk).toUpperCase();
        //System.out.println("---" + processed);
        return processed;
    }
}
