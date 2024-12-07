package com.distributed;

import org.zeromq.ZMQ;

import java.nio.ByteBuffer;
import java.util.*;

public class Worker {

    private final int workerId;
    private int lamportClock = 0;  // Lamport clock for this worker

    public Worker(int workerId) {
        this.workerId = workerId;
    }

    public static void main(String[] args) {
        int workerId = Integer.parseInt(args[0]);  // Worker ID passed as a command-line argument
        Worker worker = new Worker(workerId);
        worker.run();
    }

    public void run() {
        try (ZMQ.Context context = ZMQ.context(1)) {
            // Each worker listens on a unique port for data from the Main process
            ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
            subscriber.connect("tcp://localhost:5555");  // Connect to the Main process's PUB socket
            subscriber.subscribe(workerId + "");  // Subscribe to the topic of this worker (workerId)

            // Create a PUSH socket to send processed data back to the Main process
            ZMQ.Socket publisher = context.socket(ZMQ.PUSH);
            publisher.bind("tcp://localhost:" + (5556 + workerId));  // Each worker publishes to a unique port

            while (true) {
                // Receive chunk and Lamport clock from the Main process
                byte[] data = subscriber.recv(0);
                ByteBuffer buffer = ByteBuffer.wrap(data);
                int receivedClock = buffer.getInt();  // Get Lamport clock from the received data
                byte[] chunk = new byte[buffer.remaining()];
                buffer.get(chunk);  // Get the chunk of data

                // Update Lamport clock (Lamport clock is the max of current and received + 1)
                lamportClock = Math.max(lamportClock, receivedClock) + 1;

                // Process the chunk (for now, we convert it to uppercase)
                byte[] processedChunk = processChunk(chunk);

                // Send the processed chunk back to Main
                publisher.send(processedChunk);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Process the chunk of data (for now, we convert it to uppercase)
    private byte[] processChunk(byte[] chunk) {
        // For example: just convert the bytes to uppercase
        String processed = new String(chunk).toUpperCase();
        return processed.getBytes();
    }
}
