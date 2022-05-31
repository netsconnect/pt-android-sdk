package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

public class JsonRpcMethodNotFoundException extends JsonRpcException {
    public JsonRpcMethodNotFoundException(String code, String message, String details, Throwable cause) {
        super(code, message, details, cause);
    }

    public JsonRpcMethodNotFoundException(String message) {
        super("JSONRPC_METHOD_NOT_FOUND", message, null, null);
    }

    public JsonRpcMethodNotFoundException(String message, Throwable cause) {
        super("JSONRPC_METHOD_NOT_FOUND", message, null, cause);
    }
}
