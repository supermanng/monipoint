package com.moniepoint.main.java.kvstore.network;

import java.io.Serializable;
import java.util.List;


public class Command implements Serializable {
    private static final long serialVersionUID = 1L;
    private final CommandType type;
    private final String key;
    private final byte[] value;
    private final List<String> keys;
    private final List<byte[]> values;
    private final String startKey;
    private final String endKey;

    private Command(Builder builder) {
        this.type = builder.type;
        this.key = builder.key;
        this.value = builder.value;
        this.keys = builder.keys;
        this.values = builder.values;
        this.startKey = builder.startKey;
        this.endKey = builder.endKey;
    }

    public CommandType getType() { return type; }
    public String getKey() { return key; }
    public byte[] getValue() { return value; }
    public List<String> getKeys() { return keys; }
    public List<byte[]> getValues() { return values; }
    public String getStartKey() { return startKey; }
    public String getEndKey() { return endKey; }

    public static class Builder {
        private CommandType type;
        private String key;
        private byte[] value;
        private List<String> keys;
        private List<byte[]> values;
        private String startKey;
        private String endKey;

        public Builder(CommandType type) {
            this.type = type;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder value(byte[] value) {
            this.value = value;
            return this;
        }

        public Builder keys(List<String> keys) {
            this.keys = keys;
            return this;
        }

        public Builder values(List<byte[]> values) {
            this.values = values;
            return this;
        }

        public Builder range(String startKey, String endKey) {
            this.startKey = startKey;
            this.endKey = endKey;
            return this;
        }

        public Command build() {
            validateCommand();
            return new Command(this);
        }

        private void validateCommand() {
            if (type == null) {
                throw new IllegalStateException("Command type cannot be null");
            }

            switch (type) {
                case PUT:
                    if (key == null) {
                        throw new IllegalStateException("Key cannot be null for PUT operation");
                    }
                    break;
                case READ:
                    if (key == null) {
                        throw new IllegalStateException("Key cannot be null for READ operation");
                    }
                    break;
                case BATCH_PUT:
                    if (keys == null || values == null) {
                        throw new IllegalStateException("Keys and values cannot be null for BATCH_PUT operation");
                    }
                    if (keys.size() != values.size()) {
                        throw new IllegalStateException("Keys and values must have the same size for BATCH_PUT operation");
                    }
                    break;
                case READ_RANGE:
                    if (startKey == null || endKey == null) {
                        throw new IllegalStateException("Start and end keys cannot be null for READ_RANGE operation");
                    }
                    break;
                case DELETE:
                    if (key == null) {
                        throw new IllegalStateException("Key cannot be null for DELETE operation");
                    }
                    break;
            }
        }
    }
}