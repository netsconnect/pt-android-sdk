package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

public class JsonRpcKeepaliveException extends JsonRpcException {
    public JsonRpcKeepaliveException(String code, String message, String details, Throwable cause) {
        super(code, message, details, cause);
    }

    public JsonRpcKeepaliveException(String message) {
        super("KEEPALIVE", message, null, null);
    }

    public JsonRpcKeepaliveException(String message, Throwable cause) {
        super("KEEPALIVE", message, null, cause);
    }
}
