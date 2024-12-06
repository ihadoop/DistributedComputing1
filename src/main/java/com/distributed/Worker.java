package com.distributed;

import org.zeromq.ZMQ;
import java.nio.ByteBuffer;
import java.util.*;

public class Worker {

    private final int workerId;
    private int lamportClock = 0; // Lamport clock

    public Worker(int workerId) {
        this.workerId = workerId;
    }

    public static void main(String[] args) {
        int workerId = 4; // Worker ID passed as a command-line argument
        Worker worker = new Worker(workerId);
        worker.run();
    }

    public void run() {
        try (ZMQ.Context context = ZMQ.context(1)) {
            // Subscribing to the main process's PUB channel
            ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
            subscriber.connect("tcp://localhost:5555");
            subscriber.subscribe(""); // Subscribe to all messages

            // Create a PUB socket to send processed data back to Main
            ZMQ.Socket publisher = context.socket(ZMQ.PUB);
            publisher.bind("tcp://localhost:5556"); // Workers publish processed data here

            while (true) {
                // Receive chunk and Lamport clock from the previous worker or main
                byte[] data = subscriber.recv(0);
                ByteBuffer buffer = ByteBuffer.wrap(data);
                int receivedClock = buffer.getInt();
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);

                // Update Lamport clock
                lamportClock = Math.max(lamportClock, receivedClock) + 1;

                // Process the chunk
                byte[] processedChunk = processChunk(chunk);

                // Send the processed chunk to the next worker or back to the main process
                publisher.send(processedChunk);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Process the chunk of data (this is a stub; you can modify as per the project requirements)
    private byte[] processChunk(byte[] chunk) {
        // Example: just convert the bytes to uppercase
        String processed = new String(chunk).toUpperCase();
        return processed.getBytes();
    }
}
