package com.moniepoint.main.java.kvstore.client;


import com.moniepoint.main.java.kvstore.network.Command;
import com.moniepoint.main.java.kvstore.network.CommandType;
import com.moniepoint.main.java.kvstore.network.Response;
import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;


public class Client implements AutoCloseable {
    private final Socket socket;
    private final ObjectOutputStream out;
    private final ObjectInputStream in;

    public Client(String host, int port) throws IOException {
        this.socket = new Socket(host, port);
        // Important: Create output stream first to avoid deadlock
        this.out = new ObjectOutputStream(socket.getOutputStream());
        this.in = new ObjectInputStream(socket.getInputStream());
    }

    public void put(String key, byte[] value) throws IOException, ClassNotFoundException {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        Command command = new Command.Builder(CommandType.PUT)
                .key(key)
                .value(value)
                .build();
        Response response = sendCommand(command);
        validateResponse(response, "Put");
    }

    public byte[] read(String key) throws IOException, ClassNotFoundException {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        Command command = new Command.Builder(CommandType.READ)
                .key(key)
                .build();
        Response response = sendCommand(command);
        validateResponse(response, "Read");
        return (byte[]) response.getData();
    }

    public Map<String, byte[]> readRange(String startKey, String endKey) throws IOException, ClassNotFoundException {
        if (startKey == null || endKey == null)
            throw new IllegalArgumentException("Start and end keys cannot be null");
        Command command = new Command.Builder(CommandType.READ_RANGE)
                .range(startKey, endKey)
                .build();
        Response response = sendCommand(command);
        validateResponse(response, "Range read");
        return (Map<String, byte[]>) response.getData();
    }

    public void delete(String key) throws IOException, ClassNotFoundException {
        if (key == null) throw new IllegalArgumentException("Key cannot be null");
        Command command = new Command.Builder(CommandType.DELETE)
                .key(key)
                .build();
        Response response = sendCommand(command);
        validateResponse(response, "Delete");
    }

    public void batchPut(List<String> keys, List<byte[]> values) throws IOException, ClassNotFoundException {
        if (keys == null || values == null)
            throw new IllegalArgumentException("Keys and values cannot be null");
        Command command = new Command.Builder(CommandType.BATCH_PUT)
                .keys(keys)
                .values(values)
                .build();
        Response response = sendCommand(command);
        validateResponse(response, "Batch put");
    }  
    private Response sendCommand(Command command) throws IOException, ClassNotFoundException {
        synchronized (out) {
            out.writeObject(command);
            out.flush();
        }
        Response response = (Response) in.readObject();
        if (response == null) {
            throw new IOException("Received null response from server");
        }
        return response;
    }

    private void validateResponse(Response response, String operation) throws IOException {
        if (!response.isSuccess()) {
            String errorMessage = response.getError();
            if (errorMessage == null || errorMessage.trim().isEmpty()) {
                errorMessage = "Unknown error occurred";
            }
            throw new IOException(operation + " operation failed: " + errorMessage);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            out.close();
            in.close();
            socket.close();
        } catch (IOException e) {
            throw new IOException("Error closing client: " + e.getMessage(), e);
        }
    }
}