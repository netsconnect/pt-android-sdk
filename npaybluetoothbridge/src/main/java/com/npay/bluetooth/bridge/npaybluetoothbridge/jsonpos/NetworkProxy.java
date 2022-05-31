/*
 *  JSONPOS network proxy implementation, provides NetworkConnect etc for
 *  a given JsonRpcConnection.  Connections are automatically cleaned up
 *  when the underlying JsonRpcConnection closes.
 *
 *  The network proxy maintains a connectionId -> NetworkConnection mapping.
 *  The proxy handles connect, disconnected, disconnected notify, and
 *  reasonably fair rate limited data writing.
 *
 *  There's a three-level rate limiting strategy:
 *
 *  1. JsonRpcConnection uses a rate limiter and tries to keep withing link
 *     speed limits.  Due to variability of the link speed, this is not
 *     always successful.
 *
 *  2. NetworkProxy rate limits Data notifys so that the notifys, with
 *     base-64 expansion and other overhead, are within a fraction of the
 *     link speed (e.g. 80%).  This leaves some link capacity available
 *     for keepalives and application methods.
 *
 *  3. NetworkProxy monitors the JsonRpcConnection output queue size in
 *     bytes.  If the estimated transfer time of the already queued messages
 *     is too long (several seconds), the proxy stops writing Data notifys.
 *     This may happen if the link is slower than anticipated, and backing
 *     off allows keepalives and other methods to work reasonably.
 *
 *  Data rate limiting tracks the background vs. interactive status of each
 *  connection using a simple heuristic, and prefers interactive connections
 *  when deciding which connections get write attention.  When the terminal
 *  is in non-idle state (processing a transaction) background data is further
 *  reduced to minimize latency for interactive use cases.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonpos;

import android.os.SystemClock;
import android.util.Base64;
import android.util.Log;

import com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc.JsonRpcConnection;
import com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc.JsonRpcDispatcher;
import com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc.JsonRpcInlineMethodHandler;
import com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc.JsonRpcMethodExtras;
import com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc.JsonRpcThreadMethodHandler;
import com.npay.bluetooth.bridge.npaybluetoothbridge.util.CompletableFutureSubset;
import com.npay.bluetooth.bridge.npaybluetoothbridge.util.RateLimiter;

import org.json.JSONObject;

import java.util.HashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NetworkProxy {
    private static final String logTag = "NetworkProxy";
    private static final long WRITE_TRIGGER_TIMEOUT_IDLE = 5000;
    private static final long WRITE_TRIGGER_TIMEOUT_NONIDLE = 500;
    private static final long WRITE_THROTTLE_SLEEP = 2500;
    private static final int PREFER_INTERACTIVE_LIMIT = 3;
    private static final long BACKGROUND_WRITE_INTERVAL = 500;

    private HashMap<Long, NetworkProxyConnection> connections = new HashMap<Long, NetworkProxyConnection>();
    private Long connectionIds[] = null;
    private JsonRpcConnection conn = null;
    private RateLimiter dataWriteLimiter = null;
    private CompletableFutureSubset<Void> writeTriggerFuture = new CompletableFutureSubset<Void>();
    private long linkSpeed = 0;
    private boolean started = false;
    private boolean stopped = false;
    private boolean allowConnections = false;
    private int selectPreferInteractiveCount = 0;
    private boolean terminalIsIdle = true;
    private long lastNonIdleBgWriteTime = 0;

    public NetworkProxy(JsonRpcConnection conn, RateLimiter dataWriteLimiter, long linkSpeed) {
        this.conn = conn;
        this.dataWriteLimiter = dataWriteLimiter;
        this.linkSpeed = linkSpeed;
        updateConnectionKeySet();
    }

    public void registerNetworkMethods(JsonRpcDispatcher dispatcher) {
        final NetworkProxy finalProxy = this;

        dispatcher.registerMethod("NetworkConnect", new JsonRpcThreadMethodHandler() {
            public JSONObject handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception {
                String host = params.getString("host");
                int port = params.getInt("port");
                long id = params.getLong("connection_id");

                Log.i(logTag, String.format("NetworkConnect: %d -> %s:%d", id, host, port));

                if (!allowConnections) {
                    throw new IllegalStateException("reject connection, connections not allowed by proxy at this point");
                }
                if (connections.containsKey(id)) {
                    throw new IllegalArgumentException(String.format("NetworkConnect for an already active connection id %d", id));
                }

                NetworkProxyConnection c = new NetworkProxyConnection(host, port, id, finalProxy, linkSpeed);
                connections.put(id, c);
                updateConnectionKeySet();
                c.start();
                Future connectedFut = c.getConnectedFuture();
                connectedFut.get();  // block until connected/failed

                // Connection closure is handled by the network proxy write
                // loop.  It detects a closed connection with no queued data
                // and sends the necessary NetworkDisconnected.

                return null;
            }
        });
        dispatcher.registerMethod("NetworkDisconnect", new JsonRpcThreadMethodHandler() {
            public JSONObject handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception {
                long id = params.getLong("connection_id");
                String reason = params.optString("reason");
                Log.i(logTag, String.format("NetworkDisconnect: %d", id));

                NetworkProxyConnection c = connections.get(id);
                if (c == null) {
                    throw new IllegalArgumentException(String.format("NetworkDisconnect for non-existent connection %d", id));
                }
                c.close(reason != null ? new RuntimeException(reason) : new RuntimeException("peer requested closed"));
                Future closedFut = c.getClosedFuture();
                closedFut.get();  // XXX: unclean wrapped exception when ExecutionException

                return null;
            }
        });
        dispatcher.registerMethod("Data", new JsonRpcInlineMethodHandler() {
            // Must be handled efficiently, use inline handler.
            public JSONObject handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception {
                long id = params.getLong("id");
                String data64 = params.getString("data");

                NetworkProxyConnection c = connections.get(id);
                if (c == null) {
                    throw new IllegalStateException(String.format("received Data for non-existent connection id %d", id));
                }

                try {
                    byte[] data = Base64.decode(data64, Base64.DEFAULT);
                    c.write(data);
                } catch (Exception e) {
                    Log.i(logTag, "failed to decode incoming Data, closing tcp connection", e);
                    c.close(e);
                    throw e;
                }

                return null;
            }
        });
    }

    public void setTerminalIdleState(boolean isIdle) {
        boolean trigger = (isIdle != terminalIsIdle);
        terminalIsIdle = isIdle;
        if (trigger) {
            writeTriggerFuture.complete(null);
        }
    }

    public void startNetworkProxySync() throws Exception {
        if (started) {
            throw new IllegalStateException("already started");
        }

        conn.sendRequestSync("NetworkStart", null, null);
        started = true;
        allowConnections = true;

        Thread writerThread = new Thread(new Runnable() {
            public void run() {
                Exception cause = null;
                try {
                    runWriteLoop();
                    Log.d(logTag, "write loop exited");
                } catch (Exception e) {
                    Log.d(logTag, "write loop failed", e);
                    cause = e;
                }

                forceCloseConnections(cause);
                try {
                    stopNetworkProxySync();
                } catch (Exception e) {
                    Log.d(logTag, "failed to close proxy", e);
                }
            }
        });
        writerThread.start();
    }

    public void stopNetworkProxySync() throws Exception {
        if (!started) {
            return;
        }
        if (stopped) {
            return;
        }
        stopped = true;
        allowConnections = false;
        conn.sendRequestSync("NetworkStop", null, null);
        forceCloseConnections(new RuntimeException("proxy stopping"));
    }

    private void forceCloseConnections(Throwable cause) {
        allowConnections = false;
        try {
            // Avoid concurrent modification error by getting a snapshot of
            // the key set (which should no longer grow).
            NetworkProxyConnection conns[] = connections.values().toArray(new NetworkProxyConnection[0]);
            for (NetworkProxyConnection c : conns) {
                Log.i(logTag, "closing proxy connection ID " + c.getConnectionId(), cause);
                try {
                    c.close(new RuntimeException("proxy exiting", cause));
                } catch (Exception e) {
                    Log.w(logTag, "failed to close tcp connection", e);
                }
            }
        } catch (Exception e) {
            Log.w(logTag, "failed to close tcp connections", e);
        }
    }

    // True if connection needs to write to JSONPOS, either data or a
    // NetworkDisconnected message.
    private boolean connectionNeedsWrite(NetworkProxyConnection c) {
        return c.hasPendingData() || c.isClosed();
    }

    // Select a network connection next serviced for a write.  This selection
    // must ensure rough fairness (= all connections get data transfer), and
    // should ideally favor connections that seem interactive.  Also closed
    // connections must be selected so that NetworkDisconnected gets sent.
    //
    // Current approach: on most writes prefer interactive-looking connections
    // over non-interactive ones.  Use a 'last selected for write' timestamp
    // to round robin over connections, i.e. we select the connection which
    // has least recently received attention.
    private NetworkProxyConnection selectConnectionHelper(boolean interactiveOnly) {
        NetworkProxyConnection best = null;
        long oldest = -1;
        Long[] keys = connectionIds;

        for (int idx = 0; idx < keys.length; idx++) {
            long connId = keys[idx];
            NetworkProxyConnection c = connections.get(connId);
            if (c != null && connectionNeedsWrite(c) &&
                (!interactiveOnly || c.seemsInteractive())) {
                if (oldest < 0 || c.lastWriteAttention < oldest) {
                    best = c;
                    oldest = c.lastWriteAttention;
                }
            }
        }

        if (best != null) {
            best.lastWriteAttention = SystemClock.uptimeMillis();
        }
        return best;
    }

    private NetworkProxyConnection selectConnectionForWrite() {
        // To ensure rough fairness run a looping index over the connection
        // ID set.  The set may change so we may skip or process a certain
        // ID twice, but this happens very rarely in practice so it doesn't
        // matter.
        Long[] keys = connectionIds;
        NetworkProxyConnection res = null;
        if (selectPreferInteractiveCount < PREFER_INTERACTIVE_LIMIT) {
            res = selectConnectionHelper(true);
            if (res != null) {
                Log.d(logTag, String.format("select interactive connection %d for data write", res.getConnectionId()));
                selectPreferInteractiveCount++;
                return res;
            }
        }
        selectPreferInteractiveCount = 0;

        // When terminal is not idle, send data more slowly but still keep
        // sending it e.g. once or twice second to avoid HTTP activity
        // timeouts.
        if (!terminalIsIdle) {
            long now = SystemClock.uptimeMillis();
            if (now - lastNonIdleBgWriteTime < BACKGROUND_WRITE_INTERVAL) {
                Log.d(logTag, "terminal is not idle, don't send background data too often");
                return null;
            }
            lastNonIdleBgWriteTime = now;
        }

        res = selectConnectionHelper(false);
        if (res != null) {
            Log.d(logTag, String.format("select connection %d for data write", res.getConnectionId()));
            return res;
        }

        //Log.v(logTag, "no connection in need of writing, skip write");
        return null;
    }

    private void runWriteLoop() throws Exception {
        for (;;) {
            //Log.v(logTag, "network proxy write loop");
            if (conn.isClosed()) {
                Log.d(logTag, "network proxy write loop exiting, jsonrpc connection is closed");
                break;
            }
            if (stopped) {
                Log.d(logTag, "network proxy write loop exiting, stopped==true");
                break;
            }

            // If underlying JsonRpcConnection has too much queued data,
            // stop writing for a while because we don't want the queue
            // to become too large.  This is a backstop which tries to
            // ensure that the connection remains minimally responsible
            // even if Data rate limiting doesn't correctly match assumed
            // connection speed.
            if (throttleJsonRpcData()) {
                Log.d(logTag, "connection queue too long, throttle proxy writes");
                SystemClock.sleep(WRITE_THROTTLE_SLEEP);
                continue;
            }

            boolean wrote = false;
            NetworkProxyConnection c = selectConnectionForWrite();

            if (c != null) {
                byte[] data = c.getQueuedReadData();
                if (data != null) {
                    // Queue data to be written to JSONRPC.  Here we assume the caller is
                    // only providing us with reasonably small chunks (see read buffer size
                    // in NetworkProxyConnection) so that they can be written out as individual
                    // Data notifys without merging or splitting.  Consume rate limit after
                    // sending the notify to minimize latency.

                    //Log.v(logTag, String.format("connection id %d has data (chunk is %d bytes)", id, data.length));

                    JSONObject params = new JSONObject();
                    params.put("id", c.getConnectionId());
                    params.put("data", Base64.encodeToString(data, Base64.NO_WRAP));
                    conn.sendNotifySync("Data", params);

                    if (dataWriteLimiter != null) {
                        dataWriteLimiter.consumeSync(data.length);  // unencoded size
                    }

                    wrote = true;
                } else if (c.isClosed()) {
                    // Connection has no data and is closed, issue NetworkDisconnected
                    // and stop tracking.

                    //Log.v(logTag, String.format("connection id %d has no data and is closed -> send NetworkDisconnected", id));

                    String reason = null;
                    Future<Exception> closedFut = c.getClosedFuture();
                    try {
                        Exception e = closedFut.get();
                        reason = e.toString();
                    } catch (Exception e) {
                        reason = "failed to get reason: " + e.toString();
                    }

                    connections.remove(c.getConnectionId());
                    updateConnectionKeySet();

                    JSONObject params = new JSONObject();
                    params.put("connection_id", c.getConnectionId());
                    if (reason != null) {
                        params.put("reason", reason);
                    }

                    // Result is ignored for now.  We could maybe retry on error
                    // but there's no known reason for this to fail.
                    conn.sendRequestAsync("NetworkDisconnected", params);

                    wrote = true;
                }
            }

            // XXX: Support for non-idle mode should be improved using a proper
            // blocking rate limiter.  For now, when terminal is non-idle, sleep
            // only a short interval and recheck background data writing between
            // sleeps.

            // If we didn't write data, wait for a trigger future or
            // sanity timeout.
            if (!wrote) {
                if (writeTriggerFuture.isDone()) {
                    //Log.v(logTag, "refresh network proxy write trigger future");
                    writeTriggerFuture = new CompletableFutureSubset<Void>();
                }
                //Log.v(logTag, "proxy did not write data, wait for trigger");
                try {
                    long timeout = terminalIsIdle ? WRITE_TRIGGER_TIMEOUT_IDLE : WRITE_TRIGGER_TIMEOUT_NONIDLE;
                    writeTriggerFuture.get(timeout, TimeUnit.MILLISECONDS);
                } catch (TimeoutException e) {
                    /* No trigger, sanity poll. */
                }
            }
        }
    }

    public boolean throttleJsonRpcData() {
        // If underlying JsonRpcConnection write queue is too long (estimated
        // to be several seconds long, making it likely a _Keepalive would
        // fail), throttle writing Data to the JsonRpcConnection.
        long throttleLimit = linkSpeed * 2;  // 2 seconds of data (at estimated link speed)
        long queuedBytes = conn.getWriteQueueBytes();
        return queuedBytes >= throttleLimit;
    }

    private void updateConnectionKeySet() {
        Long keys[] = connections.keySet().toArray(new Long[0]);
        connectionIds = keys;
    }

    public void triggerWriteCheck() {
        writeTriggerFuture.complete(null);
    }
}
