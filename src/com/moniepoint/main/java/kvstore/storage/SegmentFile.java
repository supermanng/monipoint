package com.moniepoint.main.java.kvstore.storage;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SegmentFile implements AutoCloseable {
    private final Path filePath;
    private final FileChannel channel;
    private final Map<String, Long> index;
    private long currentOffset;
    private static final int MAX_SEGMENT_SIZE = 1024 * 1024; // 1MB

    public SegmentFile(Path filePath) throws IOException {
        this.filePath = filePath;
        this.index = new ConcurrentHashMap<>();
        this.channel = FileChannel.open(filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        rebuildIndex();
    }

    private void rebuildIndex() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        currentOffset = 0;

        while (currentOffset < channel.size()) {
            buffer.clear();
            channel.read(buffer, currentOffset);
            buffer.flip();
            int keyLength = buffer.getInt();

            ByteBuffer keyBuffer = ByteBuffer.allocate(keyLength);
            channel.read(keyBuffer, currentOffset + 4);
            String key = new String(keyBuffer.array());

            buffer.clear();
            channel.read(buffer, currentOffset + 4 + keyLength);
            buffer.flip();
            int valueLength = buffer.getInt();

            index.put(key, currentOffset + 8 + keyLength);
            currentOffset += 8 + keyLength + valueLength;
        }
    }

    public boolean write(String key, byte[] value) throws IOException {
        byte[] keyBytes = key.getBytes();
        int recordSize = 8 + keyBytes.length + (value != null ? value.length : 0);

        if (currentOffset + recordSize > MAX_SEGMENT_SIZE) {
            return false;
        }

        ByteBuffer buffer = ByteBuffer.allocate(recordSize);
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(value != null ? value.length : 0);
        if (value != null) {
            buffer.put(value);
        }
        buffer.flip();

        synchronized (this) {
            channel.write(buffer, currentOffset);
            index.put(key, currentOffset + 8 + keyBytes.length);
            currentOffset += recordSize;
            channel.force(true);
        }

        return true;
    }

    public byte[] read(String key) throws IOException {
        Long valueOffset = index.get(key);
        if (valueOffset == null) {
            return null;
        }

        synchronized (this) {
            ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
            channel.read(lengthBuffer, valueOffset - 4);
            lengthBuffer.flip();
            int valueLength = lengthBuffer.getInt();

            if (valueLength == 0) {
                return null;
            }

            ByteBuffer valueBuffer = ByteBuffer.allocate(valueLength);
            channel.read(valueBuffer, valueOffset);
            return valueBuffer.array();
        }
    }

    public Map<String, byte[]> readRange(String startKey, String endKey) throws IOException {
        Map<String, byte[]> results = new ConcurrentHashMap<>();

        for (Map.Entry<String, Long> entry : index.entrySet()) {
            String key = entry.getKey();
            if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) <= 0) {
                byte[] value = read(key);
                if (value != null) {
                    results.put(key, value);
                }
            }
        }

        return results;
    }

    public int getSegmentId() {
        String fileName = filePath.getFileName().toString();
        return Integer.parseInt(fileName.substring(8));
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
