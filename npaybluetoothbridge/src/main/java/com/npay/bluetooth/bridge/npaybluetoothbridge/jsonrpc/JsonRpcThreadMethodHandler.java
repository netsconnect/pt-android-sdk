/*
 *  Method handler executing in its own Thread so may block as necessary.
 *  Caller (JsonRpcConnection) launches a new Thread per request.  A null
 *  return value is allowed and represents an empty object, {}.
 *
 *  This is the recommended handler variant unless spawning a Thread for
 *  each request has too much of a performance impact.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

import org.json.JSONObject;

public interface JsonRpcThreadMethodHandler extends JsonRpcMethodHandler {
    JSONObject handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception;
}
