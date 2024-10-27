// Main.java
package com.moniepoint.main.java.kvstore;

import com.moniepoint.main.java.kvstore.network.Server;
import com.moniepoint.main.java.kvstore.client.Client;
import com.moniepoint.main.java.kvstore.storage.StorageEngine;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;


public class Main {
    private static final int PORT = 9999;
    private static final String DATA_DIR = "./data";

    public static void main(String[] args) {
        StorageEngine storage = null;
        Server server = null;
        Thread serverThread = null;

        try {
            // 1. Initialize Storage Engine
            System.out.println("Initializing Storage Engine...");
            storage = new StorageEngine(DATA_DIR);

            // 2. Create and Start Network Server
            System.out.println("Starting Network Server on port " + PORT);
            server = new Server(PORT, storage);

            // Create a separate thread for the server
            CountDownLatch serverStarted = new CountDownLatch(1);
            Server finalServer = server;

            serverThread = new Thread(() -> {
                try {
                    System.out.println("Server thread starting...");
                    serverStarted.countDown(); // Signal server is ready
                    finalServer.start(); // This blocks until server is stopped
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            serverThread.start();

            // Wait for server to start
            serverStarted.await();
            System.out.println("Server is ready to accept connections");

            // 3. Demonstrate Client Operations
            demonstrateClientOperations();

        } catch (Exception e) {
            System.err.println("Error in main: " + e.getMessage());
            e.printStackTrace();
        } finally {
            cleanup(storage, server, serverThread);
        }
    }

    private static void demonstrateClientOperations() {
        try (Client client = new Client("localhost", PORT)) {
            System.out.println("\n=== Starting Client Operations ===");

            // Single Put Operation
            String key = "user:1001";
            String value = "{\"name\":\"John Doe\",\"email\":\"john@example.com\"}";
            client.put(key, value.getBytes(StandardCharsets.UTF_8));
            System.out.println("✓ Put operation completed for key: " + key);

            // Read back the value we just put
            try {
                byte[] readValue = client.read(key);  // Using the same key variable
                if (readValue != null) {
                    System.out.println("✓ Read value: " + new String(readValue, StandardCharsets.UTF_8));
                } else {
                    System.out.println("! No value found for key: " + key);
                }
            } catch (IOException e) {
                System.err.println("Error reading value: " + e.getMessage());
                throw e;
            }

            // Batch Put Operation
            List<String> keys = Arrays.asList(
                    "user:1002",
                    "user:1003",
                    "user:1004"
            );

            List<byte[]> values = Arrays.asList(
                    "{\"name\":\"Jane Smith\",\"email\":\"jane@example.com\"}".getBytes(StandardCharsets.UTF_8),
                    "{\"name\":\"Bob Johnson\",\"email\":\"bob@example.com\"}".getBytes(StandardCharsets.UTF_8),
                    "{\"name\":\"Alice Brown\",\"email\":\"alice@example.com\"}".getBytes(StandardCharsets.UTF_8)
            );

            client.batchPut(keys, values);
            System.out.println("✓ Batch put operation completed for " + keys.size() + " keys");

            // Read Range Operation
            Map<String, byte[]> rangeValues = client.readRange("user:1001", "user:1004");
            System.out.println("\n=== Range Read Results ===");
            rangeValues.forEach((k, v) ->
                    System.out.println(k + " → " + new String(v, StandardCharsets.UTF_8))
            );

            // Delete Operation
            String keyToDelete = "user:1002";
            client.delete(keyToDelete);
            System.out.println("\n✓ Delete operation completed for key: " + keyToDelete);

            // Verify deletion
            try {
                byte[] deletedValue = client.read(keyToDelete);
                System.out.println("Value after deletion: " +
                        (deletedValue == null ? "null (successfully deleted)" : "still exists!"));
            } catch (IOException e) {
                if (e.getMessage().contains("Key not found")) {
                    System.out.println("✓ Key successfully deleted");
                } else {
                    throw e;
                }
            }

        } catch (Exception e) {
            System.err.println("Error in client operations: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void cleanup(StorageEngine storage, Server server, Thread serverThread) {
        System.out.println("\n=== Cleaning up resources ===");

        try {
            if (server != null) {
                System.out.println("Stopping server...");
                server.stop();
            }

            if (serverThread != null) {
                System.out.println("Waiting for server thread to terminate...");
                serverThread.join(5000); // Wait up to 5 seconds for clean shutdown
            }

            if (storage != null) {
                System.out.println("Closing storage engine...");
                storage.close();
            }

            System.out.println("Cleanup completed successfully");

        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
            e.printStackTrace();
        }
    }
}