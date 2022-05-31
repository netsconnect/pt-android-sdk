package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

public class JsonRpcInvalidRequestException extends JsonRpcException {
    public JsonRpcInvalidRequestException(String code, String message, String details, Throwable cause) {
        super(code, message, details, cause);
    }

    public JsonRpcInvalidRequestException(String message) {
        super("JSONRPC_INVALID_REQUEST", message, null, null);
    }

    public JsonRpcInvalidRequestException(String message, Throwable cause) {
        super("JSONRPC_INVALID_REQUEST", message, null, cause);
    }
}
