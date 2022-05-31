/*
 *  JSONRPC method dispatcher.  Provides inbound method/notify registration
 *  based on method name.  Handlers are sub-interfaces of JsonRpcMethodHandler,
 *  providing different call styles (inline, thread-based, etc).
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

import android.util.Log;

import java.util.HashMap;

public class JsonRpcDispatcher {
    private static final String logTag = "JsonRpcDispatcher";

    HashMap<String, JsonRpcMethodHandler> methods = new HashMap<String, JsonRpcMethodHandler>();

    public JsonRpcDispatcher() {
    }

    public void registerMethod(String method, JsonRpcMethodHandler handler) {
        if (method == null) {
            throw new IllegalArgumentException("method must be a string");
        }
        if (handler == null) {
            throw new IllegalArgumentException("null handler");
        }

        synchronized (this) {
            if (methods.containsKey(method)) {
                Log.i(logTag, String.format("method %s already registered, replacing with new handler", method));
            }
            Log.d(logTag, String.format("registered method %s", method));
            methods.put(method, handler);
        }
    }

    public void unregisterMethod(String method) {
        if (method == null) {
            throw new IllegalArgumentException("method must be a string");
        }

        synchronized (this) {
            if (methods.containsKey(method)) {
                Log.d(logTag, String.format("unregistered method %s", method));
                methods.remove(method);
            } else {
                Log.i(logTag, String.format("trying to unregister non-existent method %s, ignoring", method));
            }
        }
    }

    public boolean hasMethod(String method) {
        if (method == null) {
            throw new IllegalArgumentException("method must be a string");
        }

        synchronized (this) {
            return methods.containsKey(method);
        }
    }

    public JsonRpcMethodHandler getHandler(String method) {
        if (method == null) {
            throw new IllegalArgumentException("method must be a string");
        }

        synchronized (this) {
            return methods.get(method);
        }
    }
}
