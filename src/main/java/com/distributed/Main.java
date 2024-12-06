package com.distributed;

import org.zeromq.ZMQ;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    private static final int NUM_WORKERS = 5;
    private int lamportClock = 0; // Lamport clock

    public static void main(String[] args) {
        Main main = new Main();
        main.run();
    }

    public void run() {
        try {
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

            // Setup ZeroMQ context and PUB socket
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket publisher = context.socket(ZMQ.PUB);
            publisher.bind("tcp://*:5555");

            // Send chunks randomly to workers
            Random random = new Random();
            ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS);
            for (int i = 0; i < NUM_WORKERS; i++) {
                int workerId = i;
                executor.submit(() -> {
                    try {
                        for (byte[] chunk : fileChunks) {
                            if (random.nextInt(NUM_WORKERS) == workerId) {
                                lamportClock++; // Increment Lamport clock
                                publisher.send(serializeChunk(chunk, lamportClock));
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            }
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);

            // Wait 15 seconds before collecting results
            System.out.println("Waiting 15 seconds before collecting results...");
            Thread.sleep(15000);

            // Setup ZeroMQ subscription to collect data from workers
            ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
            subscriber.connect("tcp://localhost:5556");
            subscriber.subscribe(""); // Subscribe to all messages from workers

            // Collect processed data from workers
            List<byte[]> collectedData = new ArrayList<>();
            for (int i = 0; i < NUM_WORKERS; i++) {
                byte[] result = subscriber.recv(0);
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
            subscriber.close();
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
        buffer.putInt(lamportClock);
        buffer.put(chunk);
        return buffer.array();
    }
}
