package com.moniepoint.main.java.kvstore.storage;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class StorageEngine implements AutoCloseable {
    private final Path dataDir;
    private final ConcurrentNavigableMap<String, byte[]> memTable;
    private final List<SegmentFile> segments;
    private SegmentFile currentSegment;
    private final ReadWriteLock lock;
    private final int memTableMaxSize;
    private static final byte[] TOMBSTONE = new byte[0]; // Marker for deleted entries

    public StorageEngine(String dataDirPath) throws IOException {
        this.dataDir = Paths.get(dataDirPath);
        this.memTable = new ConcurrentSkipListMap<>();
        this.segments = Collections.synchronizedList(new ArrayList<>());
        this.lock = new ReentrantReadWriteLock();
        this.memTableMaxSize = 1024 * 1024; // 1MB
        initialize();
    }

    private void initialize() throws IOException {
        Files.createDirectories(dataDir);
        loadExistingSegments();
        if (segments.isEmpty()) {
            createNewSegment();
        }
    }

    private void loadExistingSegments() throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "segment_*")) {
            for (Path path : stream) {
                segments.add(new SegmentFile(path));
            }
        }
        segments.sort(Comparator.comparing(SegmentFile::getSegmentId));
        if (!segments.isEmpty()) {
            currentSegment = segments.get(segments.size() - 1);
        }
    }

    private void createNewSegment() throws IOException {
        int segmentId = segments.size();
        Path segmentPath = dataDir.resolve(String.format("segment_%06d", segmentId));
        currentSegment = new SegmentFile(segmentPath);
        segments.add(currentSegment);
    }

    public void put(String key, byte[] value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        lock.writeLock().lock();
        try {
            // Use TOMBSTONE for null values
            byte[] valueToStore = (value == null) ? TOMBSTONE : value;
            memTable.put(key, valueToStore);

            if (memTable.size() >= memTableMaxSize) {
                flushMemTable();
            }

            if (!currentSegment.write(key, valueToStore)) {
                createNewSegment();
                currentSegment.write(key, valueToStore);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public byte[] read(String key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        lock.readLock().lock();
        try {
            // Check memTable first
            byte[] value = memTable.get(key);
            if (value != null) {
                return Arrays.equals(value, TOMBSTONE) ? null : value;
            }

            // Search segments in reverse order (newest first)
            for (int i = segments.size() - 1; i >= 0; i--) {
                value = segments.get(i).read(key);
                if (value != null) {
                    return Arrays.equals(value, TOMBSTONE) ? null : value;
                }
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Map<String, byte[]> readRange(String startKey, String endKey) throws IOException {
        if (startKey == null || endKey == null) {
            throw new IllegalArgumentException("Start and end keys cannot be null");
        }

        lock.readLock().lock();
        try {
            Map<String, byte[]> results = new TreeMap<>();

            // Get values from memTable
            memTable.subMap(startKey, true, endKey, true)
                    .forEach((k, v) -> {
                        if (!Arrays.equals(v, TOMBSTONE)) {
                            results.put(k, v);
                        }
                    });

            // Search segments in reverse order
            for (int i = segments.size() - 1; i >= 0; i--) {
                Map<String, byte[]> segmentValues = segments.get(i).readRange(startKey, endKey);
                segmentValues.forEach((k, v) -> {
                    if (!results.containsKey(k) && !Arrays.equals(v, TOMBSTONE)) {
                        results.put(k, v);
                    }
                });
            }

            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void delete(String key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        lock.writeLock().lock();
        try {
            // Write a tombstone marker instead of null
            put(key, TOMBSTONE);
            // Remove from memTable if present
            memTable.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void batchPut(List<String> keys, List<byte[]> values) throws IOException {
        if (keys == null || values == null) {
            throw new IllegalArgumentException("Keys and values cannot be null");
        }
        if (keys.size() != values.size()) {
            throw new IllegalArgumentException("Keys and values must have the same size");
        }

        lock.writeLock().lock();
        try {
            for (int i = 0; i < keys.size(); i++) {
                put(keys.get(i), values.get(i));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void flushMemTable() throws IOException {
        if (memTable.isEmpty()) {
            return;
        }

        createNewSegment();
        for (Map.Entry<String, byte[]> entry : memTable.entrySet()) {
            if (!currentSegment.write(entry.getKey(), entry.getValue())) {
                createNewSegment();
                currentSegment.write(entry.getKey(), entry.getValue());
            }
        }
        memTable.clear();
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            flushMemTable();
            for (SegmentFile segment : segments) {
                segment.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}