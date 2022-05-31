package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

public class JsonRpcInvalidParamsException extends JsonRpcException {
    public JsonRpcInvalidParamsException(String code, String message, String details, Throwable cause) {
        super(code, message, details, cause);
    }

    public JsonRpcInvalidParamsException(String message) {
        super("JSONRPC_INVALID_PARAMS", message, null, null);
    }

    public JsonRpcInvalidParamsException(String message, Throwable cause) {
        super("JSONRPC_INVALID_PARAMS", message, null, cause);
    }
}
