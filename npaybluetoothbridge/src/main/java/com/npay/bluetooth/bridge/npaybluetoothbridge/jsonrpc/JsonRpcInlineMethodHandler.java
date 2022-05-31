/*
 *  Method handler executing in the calling code's Thread.  Handler MUST NOT
 *  block.  A null return value is allowed and represents an empty object, {}.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

import org.json.JSONObject;

public interface JsonRpcInlineMethodHandler extends JsonRpcMethodHandler {
    JSONObject handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception;
}
