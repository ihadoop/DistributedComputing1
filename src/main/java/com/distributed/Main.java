package com.distributed;

import org.zeromq.ZMQ;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    private static final int NUM_WORKERS = 5;  // Total number of workers
    private int lamportClock = 0;  // Lamport clock for the main process

    public static void main(String[] args) {
        Main main = new Main();
        main.run();
    }

    public void run() {
        try {
            // Initialize ZeroMQ context
            ZMQ.Context context = ZMQ.context(1);

            // Create a publisher socket to send chunks to workers
            ZMQ.Socket publisher = context.socket(ZMQ.PUB);
            publisher.bind("tcp://*:5555");  // Main process binds to 5555 to send chunks to workers

            // Allow workers to connect before sending data
            Thread.sleep(1000);  // Small delay to allow workers to connect

            // Create multiple subscriber sockets to receive data from each worker
            List<ZMQ.Socket> subscribers = new ArrayList<>();
            for (int i = 0; i < NUM_WORKERS; i++) {
                ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
                subscriber.connect("tcp://localhost:" + (5556 + i));  // Main process subscribes to workers' unique ports
                subscriber.subscribe("");  // Subscribe to all topics from this worker
                subscribers.add(subscriber);
            }

            // Prompt user for the file path
            Scanner scanner = new Scanner(System.in);
            System.out.println("Enter the path of the file:");
            String filePath = scanner.nextLine();

            // Read the file into byte array
            File file = new File(filePath);
            byte[] fileBytes = new byte[(int) file.length()];
            try (FileInputStream fileInputStream = new FileInputStream(file)) {
                fileInputStream.read(fileBytes);
            }

            // Split the file into 10-byte chunks
            List<byte[]> fileChunks = splitFileIntoChunks(fileBytes, 10);

            // Send chunks to workers
            ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);
            for (byte[] chunk : fileChunks) {
                executor.submit(() -> {
                    try {
                        // Randomly select a worker (workerId)
                        int workerId = new Random().nextInt(NUM_WORKERS);
                        lamportClock++;  // Increment Lamport clock
                        System.out.println("Sending chunk to worker " + workerId);

                        // Connect to the worker's unique port (5556 + workerId)
                        ZMQ.Socket workerPublisher = context.socket(ZMQ.PUB);
                        workerPublisher.connect("tcp://localhost:" + (5556 + workerId));  // Each worker listens on a unique port
                        workerPublisher.send(workerId + " " + serializeChunk(chunk, lamportClock));  // Send chunk to worker with topic as workerId
                        workerPublisher.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }

            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            // Wait for 15 seconds before collecting results
            System.out.println("Waiting 15 seconds before collecting results...");
            Thread.sleep(15000);

            // Collect processed data from workers
            List<byte[]> collectedData = new ArrayList<>();
            for (ZMQ.Socket subscriber : subscribers) {
                byte[] result = subscriber.recv(0);  // Receive processed chunk from worker
                collectedData.add(result);
            }

            // Combine all collected data
            byte[] combinedData = combineData(collectedData);

            // Save the data to a new file on the Desktop
            File outputFile = new File(System.getProperty("user.home") + "/Desktop/received_file.txt");
            try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                fos.write(combinedData);
                System.out.println("Data collected and saved to: " + outputFile.getAbsolutePath());
            }

            // Close ZeroMQ sockets
            publisher.close();
            for (ZMQ.Socket subscriber : subscribers) {
                subscriber.close();
            }
            context.term();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<byte[]> splitFileIntoChunks(byte[] fileBytes, int chunkSize) {
        List<byte[]> chunks = new ArrayList<>();
        for (int i = 0; i < fileBytes.length; i += chunkSize) {
            int end = Math.min(i + chunkSize, fileBytes.length);
            byte[] chunk = Arrays.copyOfRange(fileBytes, i, end);
            chunks.add(chunk);
        }
        return chunks;
    }

    private byte[] combineData(List<byte[]> collectedData) {
        int totalSize = collectedData.stream().mapToInt(arr -> arr.length).sum();
        byte[] combinedData = new byte[totalSize];
        int currentPosition = 0;
        for (byte[] chunk : collectedData) {
            System.arraycopy(chunk, 0, combinedData, currentPosition, chunk.length);
            currentPosition += chunk.length;
        }
        return combinedData;
    }

    // Serialize the chunk and attach the Lamport clock value
    private byte[] serializeChunk(byte[] chunk, int lamportClock) {
        ByteBuffer buffer = ByteBuffer.allocate(chunk.length + Integer.BYTES);
        buffer.putInt(lamportClock);  // Add Lamport clock (4 bytes)
        buffer.put(chunk);  // Add the chunk
        return buffer.array();
    }
}
