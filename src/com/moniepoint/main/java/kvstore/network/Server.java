package com.moniepoint.main.java.kvstore.network;

import com.moniepoint.main.java.kvstore.storage.StorageEngine;


import java.io.*;
import java.net.*;
import java.util.concurrent.*;


public class Server {
    private final ServerSocket serverSocket;
    private final StorageEngine storage;
    private final ExecutorService executorService;
    private volatile boolean running;

    public Server(int port, StorageEngine storage) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.storage = storage;
        this.executorService = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2
        );
        this.running = true;
    }

    public void start() {
        while (running) {
            try {
                Socket clientSocket = serverSocket.accept();
                executorService.submit(new ClientHandler(clientSocket, storage));
                System.out.println("New client connected from: " + clientSocket.getInetAddress());
            } catch (IOException e) {
                if (running) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void stop() {
        running = false;
        executorService.shutdown();
        try {
            // Wait for existing tasks to terminate
            if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
            serverSocket.close();
        } catch (IOException | InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private final StorageEngine storage;
        private final ObjectInputStream in;
        private final ObjectOutputStream out;
        private static final int MAX_RETRIES = 3;

        public ClientHandler(Socket socket, StorageEngine storage) throws IOException {
            this.clientSocket = socket;
            this.storage = storage;
            this.out = new ObjectOutputStream(socket.getOutputStream());
            this.in = new ObjectInputStream(socket.getInputStream());
        }

        @Override
        public void run() {
            int retryCount = 0;
            try {
                while (!Thread.interrupted() && retryCount < MAX_RETRIES) {
                    try {
                        Command command = (Command) in.readObject();
                        System.out.println("Received command: " + commandToString(command));

                        if (command == null) {
                            System.out.println("Received null command, skipping...");
                            continue;
                        }

                        processCommand(command);
                        retryCount = 0; // Reset counter on successful processing
                    } catch (ClassNotFoundException e) {
                        System.err.println("Error deserializing command: " + e.getMessage());
                        retryCount++;
                    } catch (EOFException e) {
                        System.out.println("Client disconnected normally");
                        break;
                    } catch (IOException e) {
                        System.err.println("IO error processing command: " + e.getMessage());
                        retryCount++;
                    }
                }
            } finally {
                closeConnection();
            }
        }

        private String commandToString(Command command) {
            if (command == null) return "null";
            StringBuilder sb = new StringBuilder()
                    .append("Command{type=").append(command.getType());

            switch (command.getType()) {
                case PUT:
                    sb.append(", key='").append(command.getKey()).append("'");
                    break;
                case READ:
                    sb.append(", key='").append(command.getKey()).append("'");
                    break;
                case READ_RANGE:
                    sb.append(", startKey='").append(command.getStartKey())
                            .append("', endKey='").append(command.getEndKey()).append("'");
                    break;
                case DELETE:
                    sb.append(", key='").append(command.getKey()).append("'");
                    break;
                case BATCH_PUT:
                    sb.append(", keysCount=").append(command.getKeys() != null ? command.getKeys().size() : 0);
                    break;
            }
            sb.append("}");
            return sb.toString();
        }

        private void processCommand(Command command) throws IOException {
            try {
                validateCommand(command);

                switch (command.getType()) {
                    case PUT:
                        storage.put(command.getKey(), command.getValue());
                        sendResponse(new Response(true, "Put successful for key: " + command.getKey()));
                        System.out.println("Put successful for key: " + command.getKey());
                        break;

                    case READ:
                        byte[] value = storage.read(command.getKey());
                        if (value == null) {
                            sendResponse(new Response(false, "Key not found: " + command.getKey()));
                            System.out.println("Key not found: " + command.getKey());
                        } else {
                            sendResponse(new Response(true, value));
                            System.out.println("Read successful for key: " + command.getKey());
                        }
                        break;

                    case READ_RANGE:
                        var rangeValues = storage.readRange(command.getStartKey(), command.getEndKey());
                        sendResponse(new Response(true, rangeValues));
                        System.out.println("Range read successful from " + command.getStartKey() + " to " + command.getEndKey());
                        break;

                    case DELETE:
                        storage.delete(command.getKey());
                        sendResponse(new Response(true, "Delete successful for key: " + command.getKey()));
                        System.out.println("Delete successful for key: " + command.getKey());
                        break;

                    case BATCH_PUT:
                        storage.batchPut(command.getKeys(), command.getValues());
                        sendResponse(new Response(true, "Batch put successful for " + command.getKeys().size() + " keys"));
                        System.out.println("Batch put successful for " + command.getKeys().size() + " keys");
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown command type: " + command.getType());
                }
            } catch (Exception e) {
                String errorMessage = e.getMessage() != null ? e.getMessage() : "Internal server error";
                System.err.println("Error processing command: " + errorMessage);
                sendResponse(new Response(false, errorMessage));
                e.printStackTrace();
            }
        }

        private void validateCommand(Command command) {
            if (command == null) {
                throw new IllegalArgumentException("Command cannot be null");
            }

            if (command.getType() == null) {
                throw new IllegalArgumentException("Command type cannot be null");
            }

            switch (command.getType()) {
                case PUT:
                case READ:
                case DELETE:
                    if (command.getKey() == null) {
                        throw new IllegalArgumentException("Key cannot be null for " + command.getType() + " operation");
                    }
                    break;

                case BATCH_PUT:
                    if (command.getKeys() == null || command.getValues() == null) {
                        throw new IllegalArgumentException("Keys and values cannot be null for BATCH_PUT operation");
                    }
                    if (command.getKeys().size() != command.getValues().size()) {
                        throw new IllegalArgumentException("Keys and values must have the same size for BATCH_PUT operation");
                    }
                    break;

                case READ_RANGE:
                    if (command.getStartKey() == null || command.getEndKey() == null) {
                        throw new IllegalArgumentException("Start and end keys cannot be null for READ_RANGE operation");
                    }
                    break;
            }
        }

        private void sendResponse(Response response) throws IOException {
            synchronized (out) {
                System.out.println("Sending response: " + response.isSuccess() +
                        (response.getError() != null ? " - " + response.getError() : ""));
                out.writeObject(response);
                out.flush();
            }
        }

        private void closeConnection() {
            try {
                System.out.println("Closing connection for client: " + clientSocket.getInetAddress());
                if (in != null) in.close();
                if (out != null) out.close();
                if (clientSocket != null) clientSocket.close();
            } catch (IOException e) {
                System.err.println("Error closing connection: " + e.getMessage());
            }
        }
    }
}