/*
 *  Additional information for inbound method handling.
 *
 *  Collected into a holder object to keep interface signatures clean, and to
 *  allow easier addition and removal of extra fields without breaking call
 *  sites.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

import org.json.JSONObject;

public class JsonRpcMethodExtras {
    /*package*/ String method;
    /*package*/ String id;
    /*package*/ JSONObject message;
    /*package*/ JsonRpcConnection connection;

    JsonRpcMethodExtras() {
    }

    public String getMethod() {
        return method;
    }

    public String getId() {
        return id;
    }

    public JSONObject getMessage() {
        return message;
    }

    public JsonRpcConnection getConnection() {
        return connection;
    }
}
