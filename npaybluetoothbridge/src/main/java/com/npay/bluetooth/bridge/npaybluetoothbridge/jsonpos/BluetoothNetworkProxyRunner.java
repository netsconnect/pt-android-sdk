/*
 *  Run a persistent JSONPOS network proxy over Bluetooth RFCOMM to a given,
 *  already paired MAC address or automatic detection of available device.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonpos;

import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothSocket;
import android.os.SystemClock;
import android.util.Log;


import com.npay.bluetooth.bridge.npaybluetoothbridge.bluetooth.BluetoothConnect;
import com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc.JsonRpcConnection;
import com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc.JsonRpcDispatcher;
import com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc.JsonRpcInlineMethodHandler;
import com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc.JsonRpcMethodExtras;
import com.npay.bluetooth.bridge.npaybluetoothbridge.util.ExceptionUtil;
import com.npay.bluetooth.bridge.npaybluetoothbridge.util.TerminalVersion;
import com.npay.bluetooth.bridge.npaybluetoothbridge.util.TokenBucketRateLimiter;

import org.json.JSONObject;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.Future;

public class BluetoothNetworkProxyRunner {
    public enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED
    }

    public interface DebugStatusCallback {
        void updateStatus(String text) throws Exception;
    }

    public interface TerminalInfoCallback {
        void terminalInfo(JsonRpcConnection conn, JSONObject terminalInfo) throws Exception;
    }

    public interface StatusCallback {
        void status(JsonRpcConnection conn, JSONObject status) throws Exception;
    }

    public interface ConnectionStateCallback {
        void connectionState(JsonRpcConnection conn, ConnectionState state) throws Exception;
    }

    private static final String logTag = "BluetoothProxy";
    // Connection retry schedule is important for reliability.  For small
    // connection drops the retry can be quick, but there must be a backoff
    // to at least 10 seconds to allow SPm20 Bluetooth init to succeed when
    // the terminal restarts.
    private static final long RETRY_SLEEP_MILLIS_MIN = 1000;
    private static final long RETRY_SLEEP_MILLIS_MAX = 10000;
    private static final long RETRY_SLEEP_MILLIS_STEP = 2000;
    private static final long RFCOMM_DISCARD_TIME = 2000;
    private static final long SYNC_TIMEOUT = 5000;
    private static final long SPM20_LINK_SPEED = 10 * 1024;  // Default without .link_speed
    private static final long MIN_LINK_SPEED = 10 * 1024;

    BluetoothConnect connecter = null;
    String bluetoothMac = null;
    DebugStatusCallback debugStatusCb = null;
    TerminalInfoCallback terminalInfoCb = null;
    StatusCallback statusCb = null;
    ConnectionStateCallback connectionStateCb = null;

    boolean currentTerminalIdle = true;
    BluetoothSocket currentBtSocket = null;
    JsonRpcConnection currentConnection = null;
    NetworkProxy currentProxy = null;

    Exception stopReason = null;

    private int failCount = 0;

    public BluetoothNetworkProxyRunner(String bluetoothMac) {
        this.bluetoothMac = bluetoothMac;  // null = autodetect
        this.connecter = new BluetoothConnect();
    }

    public void setDebugStatusCallback(DebugStatusCallback cb) {
        debugStatusCb = cb;
    }

    public void setTerminalInfoCallback(TerminalInfoCallback cb) {
        terminalInfoCb = cb;
    }

    public void setStatusCallback(StatusCallback cb) {
        statusCb = cb;
    }

    public void setConnectionStateCallback(ConnectionStateCallback cb) {
        connectionStateCb = cb;
    }

    private void updateDebugStatus(String text) {
        try {
            if (debugStatusCb != null) {
                debugStatusCb.updateStatus(text);
            }
        } catch (Exception e) {
            Log.d(logTag, "failed to update debug status, ignoring", e);
        }
    }

    private void updateTerminalInfo(JsonRpcConnection conn, JSONObject terminalInfo) {
        try {
            if (terminalInfoCb != null) {
                terminalInfoCb.terminalInfo(conn, terminalInfo);
            }
        } catch (Exception e) {
            Log.d(logTag, "failed to update terminal info, ignoring", e);
        }
    }

    private void updateStatus(JsonRpcConnection conn, JSONObject status) {
        try {
            if (statusCb != null) {
                statusCb.status(conn, status);
            }
        } catch (Exception e) {
            Log.d(logTag, "failed to update status, ignoring", e);
        }
    }

    private void updateConnectionState(JsonRpcConnection conn, ConnectionState state) {
        try {
            if (connectionStateCb != null) {
                connectionStateCb.connectionState(conn, state);
            }
        } catch (Exception e) {
            Log.d(logTag, "failed to update connection state, ignoring", e);
        }
    }

    private void closeCurrentBtSocket() {
        if (currentBtSocket != null) {
            try {
                Log.i(logTag, "closing current bluetooth socket");
                currentBtSocket.close();
            } catch (Exception e) {
                Log.i(logTag, "failed to close current bt socket, ignoring", e);
            }
        }
    }

    private void closeCurrentJsonRpcConnection(Exception reason) {
        if(currentConnection != null) {
            try {
                currentConnection.close(reason);
            } catch (Exception e) {
                Log.i(logTag, "failed to close jsonrpc connection:", e);
            }
        }

    }

    public void runProxyLoop() {
        for (;;) {
            closeCurrentJsonRpcConnection(new RuntimeException ("closing just in case"));
            closeCurrentBtSocket();

            if (stopReason != null) {
                break;
            }

            currentTerminalIdle = true;
            currentBtSocket = null;
            currentConnection = null;
            currentProxy = null;

            updateConnectionState(null, ConnectionState.DISCONNECTED);

            try {
                updateConnectionState(null, ConnectionState.CONNECTING);
                String mac = bluetoothMac != null ? bluetoothMac : autodetectTargetMac();
                updateDebugStatus("Connecting RFCOMM to " + mac);
                Future<BluetoothSocket> fut = connecter.connect(mac);
                currentBtSocket = fut.get();
                updateDebugStatus("Success, start network proxy to " + mac);
                runProxy(currentBtSocket);
            } catch (Exception e) {
                updateDebugStatus("FAILED: " + ExceptionUtil.unwrapExecutionExceptionsToThrowable(e).toString());
                Log.i(logTag, "bluetooth connect failed, sleep and retry", e);
                failCount++;
            }
            long retrySleep = Math.min(RETRY_SLEEP_MILLIS_MIN + RETRY_SLEEP_MILLIS_STEP * failCount, RETRY_SLEEP_MILLIS_MAX);
            Log.i(logTag, String.format("sleep %d ms", retrySleep));
            SystemClock.sleep(retrySleep);
        }

        closeCurrentJsonRpcConnection(stopReason);
        closeCurrentBtSocket();

        Log.i(logTag, "proxy loop stop reason set, stopping:", stopReason);
    }

    // Request proxy loop to stop.  Returns without waiting for the stop
    // to complete at present.
    public void stopProxyLoop(Exception reason) {
        if (stopReason != null) {
            return;
        }
        if (reason == null) {
            reason = new RuntimeException("proxy stop requested by caller");
        }
        Log.i(logTag, "setting proxy stop reason:", reason);
        stopReason = reason;
        closeCurrentJsonRpcConnection(reason);
    }

    // Automatic target detection based on sorted device list and filtering
    // for npay relevant devices (such as SPm20).  If multiple devices
    // are available, uses the first available, compatible device.  Unpair
    // any undesired devices manually if necessary.
    private String autodetectTargetMac() throws Exception {
        Log.i(logTag, "autodetect target MAC");
        BluetoothDevice[] devs = connecter.getAvailableDevices();
        if (devs.length == 0) {
            throw new Exception("cannot autodetect a terminal");
        }

        Log.i(logTag, "autodetected " + devs[0].getName() + " (" + devs[0].getAddress() + ")");
        return devs[0].getAddress();
    }

    // A simple, soft idle vs. non-idle heuristic based on the
    // .transaction_status field of Status.  This allows data rate
    // limiting to reduce background traffic during Purchase
    // processing.
    //
    // This is not ideal as only operations involving .transaction_status
    // are considered non-idle.  For example, DisplayScreen does not cause
    // a non-idle status to be detected.  Future terminal versions are
    // likely to indicate idle vs. non-idle state as an explicit field so
    // that this can be made more accurate.
    private void checkIdleStateChange(JSONObject status) {
        boolean isIdle = (status.optString("transaction_status", null) == null);
        if (currentTerminalIdle) {
            if (!isIdle) {
                Log.i(logTag, "terminal went from idle to non-idle");
                if (currentProxy != null) {
                    currentProxy.setTerminalIdleState(isIdle);
                }
            }
        } else {
            if (isIdle) {
                Log.i(logTag, "terminal went from non-idle to idle");
                if (currentProxy != null) {
                    currentProxy.setTerminalIdleState(isIdle);
                }
            }
        }
        currentTerminalIdle = isIdle;
    }

    private void handleJsonposStatusUpdate(JSONObject status) {
        Log.i(logTag, "STATUS: " + status.toString());
        updateDebugStatus("Status: " + status.toString());
        checkIdleStateChange(status);
    }

    private void handleJsonposStatusUpdate(JsonRpcConnection conn, JSONObject status) {
        Log.i(logTag, "STATUS: " + status.toString());
        updateDebugStatus("Status: " + status.toString());
        updateStatus(conn, status);
    }

    // Status poller for older terminals with no StatusEvent support.
    private void runStatusPoller(final JsonRpcConnection conn) throws Exception {
        new Thread(new Runnable() {
            public void run () {
                try {
                    for (;;) {
                        if (conn.isClosed()) {
                            break;
                        }
                        try {
                            JSONObject params = new JSONObject();
                            JSONObject status = conn.sendRequestSync("Status", params);
                            handleJsonposStatusUpdate(conn, status);
                        } catch (Exception e) {
                            Log.i(logTag, "Status failed, ignoring", e);
                        }
                        SystemClock.sleep(5000);
                    }
                } catch (Exception e) {
                    Log.d(logTag, "status poller failed", e);
                }
                Log.i(logTag, "status poller exiting");
            }
        }).start();
    }

    private void runProxy(BluetoothSocket btSocket) throws Exception {
        Log.i(logTag, "launch jsonrpc connection");

        InputStream btIs = btSocket.getInputStream();
        OutputStream btOs = btSocket.getOutputStream();

        // Method dispatcher for connection.
        JsonRpcDispatcher disp = new JsonRpcDispatcher();
        disp.registerMethod("StatusEvent", new JsonRpcInlineMethodHandler() {
            public JSONObject handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception {
                handleJsonposStatusUpdate(extras.getConnection(), params);
                return new JSONObject();
            }
        });

        // Create a JSONRPC connection for the input/output stream, configure
        // it, and start read/write loops.
        final JsonRpcConnection conn = new JsonRpcConnection(btIs, btOs);
        currentConnection = conn;
        conn.setKeepalive();
        //conn.setDiscard(RFCOMM_DISCARD_TIME);  // unnecessary if _Sync reply scanning is reliable
        conn.setSync(SYNC_TIMEOUT);
        conn.setDispatcher(disp);
        conn.start();
        Future<Exception> connFut = conn.getClosedFuture();
        Future<Void> readyFut = conn.getReadyFuture();

        // Wait for _Sync to complete before sending anything.
        Log.i(logTag, "wait for connection to become ready");
        readyFut.get();

        // Reset backoff.
        failCount = 0;

        updateConnectionState(conn, ConnectionState.CONNECTING);

        // Check TerminalInfo before enabling network proxy.  TerminalInfo
        // can provide useful information for choosing rate limits. Fall back
        // to VersionInfo (older terminals).  The 'android_sdk_version' is not
        // a required field, but is given informatively.
        JSONObject terminalInfo = null;
        String[] versionMethods = { "TerminalInfo", "VersionInfo" };
        for (String method : versionMethods) {
            JSONObject terminalInfoParams = new JSONObject();
            terminalInfoParams.put("android_sdk_version", com.npay.bluetooth.bridge.npaybluetoothbridge.sdk.Sdk.getSdkVersion());
            Future<JSONObject> terminalInfoFut = conn.sendRequestAsync(method, terminalInfoParams);
            try {
                terminalInfo = terminalInfoFut.get();
                Log.i(logTag, String.format("%s: %s", method, terminalInfo.toString()));
                break;
            } catch (Exception e) {
                Log.w(logTag, String.format("%s: failed, ignoring", method), e);
            }
        }
        if (terminalInfo == null) {
            Log.w(logTag, "failed to get TerminalInfo");
            terminalInfo = new JSONObject();
        }
        updateTerminalInfo(conn, terminalInfo);

        // Feature detection based on version comparison.  Terminal versions
        // numbers have the format MAJOR.MINOR.PATCH where each component is
        // a number.  Use version comparison helper class for comparisons.
        String terminalVersion = terminalInfo.optString("version", null);
        if (terminalVersion != null) {
            Log.i(logTag, "terminal software version is: " + terminalVersion);
            if (terminalVersion.equals("0.0.0")) {
                Log.w(logTag, "terminal is running an unversioned development build (0.0.0)");
            }
            try {
                if (TerminalVersion.supportsStatusEvent(terminalVersion)) {
                    Log.i(logTag, "terminal supports StatusEvent, no need for polling");
                } else {
                    Log.i(logTag, "terminal does not support StatusEvent, poll using Status request");
                    runStatusPoller(conn);
                }
            } catch (Exception e) {
                Log.w(logTag, "failed to parse terminal version", e);
            }
        } else {
            Log.w(logTag, "terminal software version is unknown");
        }

        // Rate limits are based on the known or estimated base link speed.
        // Use .link_speed from TerminalInfo response (with a sanity minimum)
        // so that SPm20 link speed differences are taken into account
        // automatically.  Assume a hardcoded default if no .link_speed is
        // available.
        long linkSpeed = terminalInfo.optLong("link_speed", SPM20_LINK_SPEED);
        linkSpeed = Math.max(linkSpeed, MIN_LINK_SPEED);
        Log.i(logTag, String.format("use base link speed %d bytes/second", linkSpeed));

        // Compute other rate limits from the base link speed.
        long jsonrpcWriteTokenRate = linkSpeed;
        long jsonrpcWriteMaxTokens = (long) ((double) jsonrpcWriteTokenRate * 0.25);  // ~250ms buffered data maximum
        long dataWriteTokenRate = (long) ((double) jsonrpcWriteTokenRate * 0.4);  // 0.5 expands by base64 to about 70-80% of link, plus overhead and headroom for other requests
        long dataWriteMaxTokens = (long) ((double) jsonrpcWriteTokenRate * 0.4 * 0.25);  // ~250ms buffered data maximum
        Log.i(logTag, String.format("using jsonrpc transport rate limits: maxTokens=%d, rate=%d", jsonrpcWriteMaxTokens, jsonrpcWriteTokenRate));
        Log.i(logTag, String.format("using Data notify rate limits: maxTokens=%d, rate=%d", dataWriteMaxTokens, dataWriteTokenRate));
        TokenBucketRateLimiter connLimiter = new TokenBucketRateLimiter("jsonrpcWrite", jsonrpcWriteMaxTokens, jsonrpcWriteTokenRate);
        TokenBucketRateLimiter dataLimiter = new TokenBucketRateLimiter("dataWrite", dataWriteMaxTokens, dataWriteTokenRate);
        conn.setWriteRateLimiter(connLimiter);

        // _Sync and other handshake steps completed, register Network*
        // methods and start networking.  This could be made faster by
        // starting the network proxy right after _Sync, and then updating
        // the rate limits once we have a TerminalInfo response.
        Log.i(logTag, "starting network proxy");
        NetworkProxy proxy = new NetworkProxy(conn, dataLimiter, jsonrpcWriteTokenRate /*linkSpeed*/);
        currentProxy = proxy;
        proxy.registerNetworkMethods(disp);
        proxy.startNetworkProxySync();
        proxy.setTerminalIdleState(currentTerminalIdle);

        updateConnectionState(conn, ConnectionState.CONNECTED);

        //com.napy.np.android.tests.JsonposTests.testFileDownloads(conn);
        //com.npay.np.android.tests.JsonposTests.testSuspend(conn);
        //com.npay.np.android.tests.JsonposTests.testTerminalVersionCompare();

        // Check for proxy stop.
        if (stopReason != null) {
            throw new RuntimeException("closing because proxy stop requested");
        }

        // Wait for JSONPOS connection to finish, due to any cause.
        Log.i(logTag, "wait for jsonrpc connection to finish");
        Exception closeReason = connFut.get();
        Log.i(logTag, "jsonrpc connection finished");
    }
}
