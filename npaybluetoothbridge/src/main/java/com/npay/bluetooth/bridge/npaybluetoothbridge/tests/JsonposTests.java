/*
 *  Some JSONPOS tests.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.tests;

import android.os.SystemClock;
import android.util.Log;

import com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc.JsonRpcConnection;

import org.json.JSONObject;

import java.util.concurrent.Future;

public class JsonposTests {
    private static final String logTag = "JsonposTests";
    public static final String DEV_API_KEY = "e086554a-9a35-4bbf-9f99-a8a7cd7d1dcc";

    // Simple test Purchase.
    public static void testPurchase(JsonRpcConnection conn) throws Exception {
        JSONObject params = new JSONObject();
        params.put("api_key", DEV_API_KEY);
        params.put("amount", 123);
        params.put("currency", "EUR");
        params.put("receipt_id", 123);
        params.put("sequence_id", 321);
        try {
            JSONObject res = conn.sendRequestSync("Purchase", params, null);
            Log.i(logTag, String.format("PURCHASE RESULT: %s", res.toString()));
        } catch (Exception e) {
            Log.i(logTag, "PURCHASE FAILED", e);
        }
    }

    // Test parallel file downloads; these should complete with a good
    // throughput, there should be no _Keepalive timeout, and the terminal
    // should remain responsive to e.g. Purchases while the downloads are
    // in progress.
    public static void testFileDownloads(JsonRpcConnection conn) throws Exception {
        SystemClock.sleep(20000);  // wait for connection to settle first
        JSONObject params = new JSONObject();
        params.put("api_key", DEV_API_KEY);
        params.put("test_id", "large_file_download");
        Future<JSONObject> res1 = conn.sendRequestAsync("Test", params, null);
        Future<JSONObject> res2 = conn.sendRequestAsync("Test", params, null);
        Future<JSONObject> res3 = conn.sendRequestAsync("Test", params, null);
        res1.get();
        res2.get();
        res3.get();
        JSONObject tmp;
        tmp = res1.get();
        Log.i(logTag, String.format("DOWNLOAD 1 RESULT: %s", tmp.toString()));
        tmp = res2.get();
        Log.i(logTag, String.format("DOWNLOAD 2 RESULT: %s", tmp.toString()));
        tmp = res3.get();
        Log.i(logTag, String.format("DOWNLOAD 3 RESULT: %s", tmp.toString()));
    }

    // Terminal Suspend test.
    public static void testSuspend(JsonRpcConnection conn) throws Exception {
        JSONObject params = new JSONObject();
        params.put("api_key", DEV_API_KEY);
        try {
            conn.sendRequestAsync("Suspend", params, null);
        } catch (Exception e) {
            Log.i(logTag, "Suspend failed", e);
        }
        SystemClock.sleep(50);
        conn.closeStreamsRaw();
        System.exit(1);
        for (;;) {}
    }

    // Test terminal version comparisons, log output only.
    public static void testTerminalVersionCompare() throws Exception {
        String versions[] = {
            "0.0.0", "2.3.3", "2.3.4", "2.3.5", "2.2.4", "2.4.4",
            "1.3.4", "3.3.4", "17.9.0", "18.2.0", "18.7.0", "18.7.50"
        };
        for (String ver1 : versions) {
            for (String ver2 : versions) {
                boolean res = com.npay.bluetooth.bridge.npaybluetoothbridge.util.TerminalVersion.greaterOrEqual(ver1, ver2);
                Log.i(logTag, String.format("version compare: %s >= %s -> %b",
                                            ver1, ver2, res));
            }
        }
    }
}
