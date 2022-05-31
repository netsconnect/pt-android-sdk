/*
 *   Marker interface for different JSONRPC method handler call styles:
 *
 *   - JsonRpcThreadMethodHandler: calling code (JsonRpcConnection) launches a
 *     new Thread per request, so the method handler can block.
 *
 *   - JsonRpcInlineMethodHandler: calling code calls handler directly, and the
 *     handler must not block.
 *
 *   - JsonRpcFutureMethodHandler: calling code calls handler directly, and the
 *     handler must return a Future which eventually completes.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

public interface JsonRpcMethodHandler {
}
