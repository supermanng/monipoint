package com.moniepoint.main.java.kvstore.network;


import java.io.Serializable;

public class Response implements Serializable {
    private final boolean success;
    private final Object data;
    private final String message;

    public Response(boolean success, Object data) {
        this.success = success;
        this.data = data;
        this.message = null;
    }

    public Response(boolean success, String error) {
        this.success = success;
        this.data = null;
        this.message = error;
    }

    public Response(boolean success, Object data, String message) {
        this.success = success;
        this.data = data;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public Object getData() {
        return data;
    }

    public String getError() {
        return message;
    }
}
