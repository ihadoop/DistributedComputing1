package com.distributed;

import java.util.Arrays;

public class ByteArrayBuffer {
    private byte[] buffer;   // The byte array that stores data
    private int position;    // Current position in the buffer
    private int limit;       // Limit position (end of buffer)

    // Constructor to initialize the buffer with a given size
    public ByteArrayBuffer(int size) {
        this.buffer = new byte[size];
        this.position = 0;
        this.limit = size;
    }

    // Constructor to wrap an existing byte array (similar to ByteBuffer.wrap())
    public ByteArrayBuffer(byte[] array) {
        this.buffer = array;
        this.position = 0;
        this.limit = array.length;
    }

    // Method to put an integer (4 bytes) into the buffer (Big Endian)
    public void putInt(int value) {
        if (position + Integer.BYTES > buffer.length) {
            throw new IndexOutOfBoundsException("Not enough space to write int.");
        }
        // Put the integer into the buffer (big-endian order)
        buffer[position++] = (byte) (value >>> 24);
        buffer[position++] = (byte) (value >>> 16);
        buffer[position++] = (byte) (value >>> 8);
        buffer[position++] = (byte) (value);
    }

    // Method to append a byte array into the buffer
    public void put(byte[] src) {
        if (position + src.length > buffer.length) {
            throw new IndexOutOfBoundsException("Not enough space to put byte array.");
        }
        // Append the byte array into the buffer
        System.arraycopy(src, 0, buffer, position, src.length);
        position += src.length;
    }

    // Method to get an integer (4 bytes) from the buffer (Big Endian)
    public int getInt() {
        if (position + Integer.BYTES > limit) {
            throw new IndexOutOfBoundsException("Not enough data to read int.");
        }
        // Read 4 bytes and convert them to an int (big-endian order)
        int value = ((buffer[position] & 0xFF) << 24) |
                ((buffer[position + 1] & 0xFF) << 16) |
                ((buffer[position + 2] & 0xFF) << 8) |
                (buffer[position + 3] & 0xFF);
        position += Integer.BYTES;  // Move the position by 4 bytes after reading the int
        return value;
    }

    // Method to get the remaining byte array from the current position
    public byte[] getRemainingBytes() {
        int remaining = remaining();
        byte[] remainingData = new byte[remaining];
        System.arraycopy(buffer, position, remainingData, 0, remaining);
        position = limit;  // Move the position to the limit (end of buffer)
        return remainingData;
    }

    // Method to get the number of remaining bytes in the buffer
    public int remaining() {
        return limit - position;
    }

    // Method to reset the buffer's position to the beginning
    public void reset() {
        position = 0;
    }

    // Method to check if there are remaining bytes in the buffer
    public boolean hasRemaining() {
        return position < limit;
    }

    // Method to get the entire byte array from the buffer (up to the current position)
    public byte[] getArray() {
        return Arrays.copyOf(buffer, position);
    }

    // Method to get the current position in the buffer
    public int getPosition() {
        return position;
    }
}
