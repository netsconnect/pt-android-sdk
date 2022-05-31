package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

public class JsonRpcParseErrorException extends JsonRpcException {
    public JsonRpcParseErrorException(String code, String message, String details, Throwable cause) {
        super(code, message, details, cause);
    }

    public JsonRpcParseErrorException(String message) {
        super("JSONRPC_PARSE_ERROR", message, null, null);
    }

    public JsonRpcParseErrorException(String message, Throwable cause) {
        super("JSONRPC_PARSE_ERROR", message, null, cause);
    }
}
