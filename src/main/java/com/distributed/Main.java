package com.distributed;

import org.zeromq.ZMQ;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

public class Main {

    private static final int NUM_WORKERS = 5;  // Total number of workers
    private int lamportClock = 0;  // Lamport clock for the main process
 private int times = 0;
    public static void main(String[] args) {
        Main main = new Main();
        main.run();
    }

    public void run() {
        try {
            // Initialize ZeroMQ context
            Publisher publisher = new Publisher(5555);
            // Prompt user for the file path
            Scanner scanner = new Scanner(System.in);
            System.out.println("Enter the path of the file:");
            String filePath = scanner.nextLine();
            //String filePath = "/Users/sunshine/Downloads/Worker.java";
            // Create multiple subscriber sockets to receive data from each worker
            //Thread.sleep(20000);
            //List<ZMQ.Socket> subscribers = new ArrayList<>();


            // Read the file into byte array
            File file = new File(filePath);
            byte[] fileBytes = new byte[(int) file.length()];
            try (FileInputStream fileInputStream = new FileInputStream(file)) {
                fileInputStream.read(fileBytes);
            }

            final List<byte[]> collectedData = new ArrayList<>();
            for (int i = 0; i < NUM_WORKERS; i++) {
                final  int workerId = i;
                CountDownLatch latch = new CountDownLatch(1);
                new Thread(() -> {
                    Subscriber subscriber = new Subscriber(workerId+"",8888+workerId);
                    latch.countDown();
                    while (true) {
                        byte[] result =  subscriber.run();  // Receive processed chunk from worker
                        collectedData.add(result);
                    }
                }).start();

                latch.await();  // Wait until countDown() is called on the latch

            }

            // Split the file into 10-byte chunks
            List<byte[]> fileChunks = splitFileIntoChunks(fileBytes, 10);

            // Send chunks to workers
            //ExecutorService executor = Executors.newFixedThreadPool(NUM_WORKERS*10);
            for (byte[] chunk : fileChunks) {

                    try {
                        // Randomly select a worker (workerId)
                        int workerId = new Random().nextInt(NUM_WORKERS);
                        lamportClock++;  // Increment Lamport clock
                        System.out.println("Sending chunk to worker " + workerId);
                        // Connect to the worker's unique port (5556 + workerId)
                        publisher.send(workerId+"", serializeChunk(chunk, lamportClock));  // Send chunk to worker with topic as workerId
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
            }

            // Wait for 15 seconds before collecting results
            System.out.println("Waiting 15 seconds before collecting results...");
            Thread.sleep(15000);



            // Combine all collected data
            byte[] combinedData = combineData(collectedData);

            // Save the data to a new file on the Desktop
            File outputFile = new File(System.getProperty("user.home") + "/Desktop/received_file.txt");
            try (FileOutputStream fos = new FileOutputStream(outputFile)) {
                fos.write(combinedData);
                System.out.println("Data collected and saved to: " + outputFile.getAbsolutePath());
            }


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
        Map<Integer,byte[]> maps =  new TreeMap<>();

        int currentPosition = 0;
        for (byte[] chunk : collectedData) {
            ByteBuffer buffer = ByteBuffer.wrap(chunk);

            int lamportClock = buffer.getInt();  // Add Lamport clock (4 bytes)

            byte [] data = new byte[buffer.remaining()];
            buffer.get(data);  // Add the
            maps.put(lamportClock, data);
        }
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        for (Map.Entry<Integer, byte[]> entry : maps.entrySet()) {
            outputStream.writeBytes(entry.getValue());

        }

        return outputStream.toByteArray();
    }

    // Serialize the chunk and attach the Lamport clock value
    private byte[] serializeChunk(byte[] chunk, int lamportClock) {
        ByteBuffer buffer = ByteBuffer.allocate(chunk.length + Integer.BYTES);
        buffer.putInt(lamportClock);  // Add Lamport clock (4 bytes)
        buffer.put(chunk);  // Add the chunk
        return buffer.array();
    }
}
