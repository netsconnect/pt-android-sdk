package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

public class JsonRpcInternalErrorException extends JsonRpcException {
    public JsonRpcInternalErrorException(String code, String message, String details, Throwable cause) {
        super(code, message, details, cause);
    }

    public JsonRpcInternalErrorException(String message) {
        super("INTERNAL_ERROR", message, null, null);
    }

    public JsonRpcInternalErrorException(String message, Throwable cause) {
        super("INTERNAL_ERROR", message, null, cause);
    }
}
