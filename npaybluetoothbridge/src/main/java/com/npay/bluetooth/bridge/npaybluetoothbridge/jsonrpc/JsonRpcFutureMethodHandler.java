/*
 *  Method handler which returns a Future<JSONObject> which must eventually
 *  complete.  A direct null return value is allowed and represents an empty
 *  object, {}.  Similarly, if the Future<JSONObject> completes with null,
 *  it represents and empty object.  The method MUST NOT block, and must
 *  return the Future (or null) promptly.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

import org.json.JSONObject;

import java.util.concurrent.Future;

public interface JsonRpcFutureMethodHandler extends JsonRpcMethodHandler {
    Future<JSONObject> handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception;
}
