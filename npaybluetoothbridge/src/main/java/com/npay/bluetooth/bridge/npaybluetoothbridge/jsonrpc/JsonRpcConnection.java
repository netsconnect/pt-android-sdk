/*
 *  Run a Npay JSONRPC connection on a given, pre-connected InputStream
 *  and OutputStream pair.  Each instance tracks one connection from start to
 *  finish and is not reused.
 *
 *  Connection lifecycle:
 *
 *    - Construct a JsonRpcConnection instance for an already connected
 *      InputStream/OutputStream pair.  Connecting the stream must be done
 *      by the caller.
 *
 *    - Call setXxx() methods to configure keepalive, method dispatcher, rate
 *      limits, etc.  Some parameters, like rate limiter, can also be changed
 *      later on-the-fly.
 *
 *    - Call start() to launch read, write, and keepalive threads, all managed
 *      by this class internally.
 *
 *    - Call waitReady() before sending any requests.  The call will block and
 *      return when the connection is ready for application requests, or throw
 *      an error if e.g. _Sync'ing the connection times out.  Calling this
 *      method is especially important if _Sync is used.  Equivalent method
 *      returning a waitable Future: getReadyFuture().
 *
 *    - Call sendRequestSync() and sendRequestAsync() to send JSONRPC requests
 *      synchronously (blocks) or asynchronously (returns Future).  Call
 *      sendNotifySync() to send JSONRPC notifies; the call never waits.
 *
 *    - If necessary, call waitClosed() to wait for the connection to be
 *      closed.  The call blocks and eventually returns or throws, regardless
 *      of what causes the connection to close (including _Sync errors,
 *      _Keepalive timeouts, peer closing the connection, local calls to
 *      close(), etc).  Future equivalent: getClosedFuture().
 *
 *    - Call close() to initiate a connection close.  The call never blocks,
 *      and eventually the closed future is set (causing waitClosed() to
 *      return).  The connection may also be closed by a keepalive timeout,
 *      remote peer closing the connection, or other external reason.  If
 *      already closed, the close() call is a safe no-op.
 *
 *    - When the connection closes for any reason, all pending futures are
 *      set to the close reason, and all blocking wait calls will exit or
 *      throw.  This closure behavior includes all lifecycle futures (i.e.
 *      started, ready, closing, closed, and their synchronous counterparts
 *      like waitClosed()), and all pending outbound request futures and
 *      their synchronous counterparts like sendRequestSync() calls.  Pending
 *      inbound method calls remain running, but their results will be ignored.
 *
 *   -  The basic idea for connection closure handling is that a caller can
 *      simply wait for e.g. an outbound request to finish (sendRequestSync()),
 *      and be guaranteed that the wait will throw if the underlying connection
 *      closes before the request completes.  There's no need to wait for the
 *      connection closure explicitly in many cases.
 *
 *  Futures and lifecycle:
 *
 *     started  -->  ready  -->  closing  -->  closed
 *
 *  Inbound requests and notifys are dispatched using a JsonRpcDispatcher.
 *  Transport layer methods (_Keepalive, _CloseReason, etc) are handled
 *  internally.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

import android.os.SystemClock;
import android.util.Log;

import com.npay.bluetooth.bridge.npaybluetoothbridge.util.CompletableFutureSubset;
import com.npay.bluetooth.bridge.npaybluetoothbridge.util.ExceptionUtil;
import com.npay.bluetooth.bridge.npaybluetoothbridge.util.InternalErrorException;
import com.npay.bluetooth.bridge.npaybluetoothbridge.util.RateLimiter;
import com.npay.bluetooth.bridge.npaybluetoothbridge.util.StatsMap;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class JsonRpcTrialParseResult {
    JSONObject msg;
    int skip;
}

public class JsonRpcConnection {
    private static final long DEFAULT_KEEPALIVE_IDLE = 30 * 1000;
    private static final long DEFAULT_KEEPALIVE_BUSY = 5 * 1000;
    private static final long KEEPALIVE_REQUEST_TIMEOUT = 5 * 1000;
    private static final long DISCARD_LOOP_DELAY = 1000;
    private static final int WRITE_CHUNK_SIZE_LIMIT = 256;  // with rate limiting
    private static final int WRITE_CHUNK_SIZE_NOLIMIT = 65536;  // without rate limiting
    private static final long WRITE_LOOP_SANITY_TIMEOUT = 10 * 1000;
    private static final long THREAD_EXIT_TIMEOUT = 5 * 1000;
    private static final long FINAL_THREAD_WAIT_TIMEOUT = 60 * 1000;
    private static long globalConnectionIdCounter = 0;

    private String logTag = "JsonRpcConnection";
    private long connectionId = 0;
    private long requestIdCounter = 0;
    private int readBufferSize = 262144;
    private int maxFrameLength = 262144 - 10;  // 10 bytes transport overhead (HHHHHHHH: and newline)
    private Thread readThread = null;
    private Thread writeThread = null;
    private Thread keepaliveThread = null;
    private InputStream connIs = null;
    private OutputStream connOs = null;
    private final ConcurrentLinkedQueue<byte[]> writeQueue = new ConcurrentLinkedQueue<byte[]>();
    private boolean keepaliveEnabled = false;
    private long keepaliveIdleInterval = 0;
    private long keepaliveBusyInterval = 0;
    private boolean discardEnabled = false;
    private long discardTime = 0;
    private boolean syncEnabled = false;
    private long syncTimeout = 0;
    private Exception closeReason = null;
    private final CompletableFutureSubset<Void> startedFuture = new CompletableFutureSubset<Void>();
    private final CompletableFutureSubset<Void> readyFuture = new CompletableFutureSubset<Void>();
    private final CompletableFutureSubset<Exception> closingFuture = new CompletableFutureSubset<Exception>();
    private final CompletableFutureSubset<Exception> closedFuture = new CompletableFutureSubset<Exception>();
    private CompletableFutureSubset<Void> writeTriggerFuture = new CompletableFutureSubset<Void>();
    private CompletableFutureSubset<Void> keepaliveTriggerFuture = new CompletableFutureSubset<Void>();
    private CompletableFutureSubset<JSONObject> pendingKeepalive = null;
    private final HashMap<String, CompletableFutureSubset<JSONObject>> pendingOutboundRequests = new HashMap<String, CompletableFutureSubset<JSONObject>>();
    private final HashMap<String, Boolean> pendingInboundRequests = new HashMap<String, Boolean>();
    private JsonRpcDispatcher dispatcher = null;
    private JsonRpcDispatcher internalDispatcher = new JsonRpcDispatcher();
    private RateLimiter writeRateLimiter = null;
    private long statsStartedTime = 0;
    private long statsReadyTime = 0;  // only set on success
    private long statsClosingTime = 0;
    private long statsClosedTime = 0;
    private long statsBytesSent = 0;
    private long statsBytesReceived = 0;  // pre-sync discarded bytes not included
    private long statsBoxesSent = 0;
    private long statsBoxesReceived = 0;  // _Sync included
    private long statsLastTime = 0;
    private long statsLogInterval = 300 * 1000;
    private final StatsMap statsOutboundRequests = new StatsMap();
    private final StatsMap statsOutboundNotifys = new StatsMap();
    private final StatsMap statsInboundRequests = new StatsMap();
    private final StatsMap statsInboundNotifys = new StatsMap();

    /*
     *  Public API
     */

    public JsonRpcConnection(InputStream is, OutputStream os) {
        if (is == null || os == null) {
            throw new IllegalArgumentException("input or output stream is null");
        }
        this.connectionId = ++globalConnectionIdCounter;
        this.logTag = String.format("JsonRpcConnection-%d", this.connectionId);
        this.connIs = is;
        this.connOs = os;
        initInternalDispatcher();
    }

    public void setKeepalive() {
        setKeepalive(DEFAULT_KEEPALIVE_IDLE, DEFAULT_KEEPALIVE_BUSY);
    }

    public void setKeepalive(long idleMillis, long busyMillis) {
        Log.d(logTag, String.format("enable automatic keepalives: idle %d ms, busy %d ms", idleMillis, busyMillis));
        this.keepaliveEnabled = true;
        this.keepaliveIdleInterval = idleMillis;
        this.keepaliveBusyInterval = busyMillis;
    }

    public void setDiscard(long discardMillis) {
        Log.d(logTag, String.format("enable automatic data discard: %d ms", discardMillis));
        this.discardEnabled = true;
        this.discardTime = discardMillis;
    }

    public void setSync(long timeoutMillis) {
        Log.d(logTag, String.format("enable connection _Sync: timeout %d ms", timeoutMillis));
        this.syncEnabled = true;
        this.syncTimeout = timeoutMillis;
    }

    public void setDispatcher(JsonRpcDispatcher dispatcher) {
        Log.d(logTag, "set dispatcher");
        this.dispatcher = dispatcher;
    }

    public void setWriteRateLimiter(RateLimiter limiter) {
        Log.d(logTag, "set write rate limiter");
        this.writeRateLimiter = limiter;
    }

    public void start() {
        startRaw();
    }

    public boolean isReady() {
        return readyFuture.isDone();
    }

    public void waitReady() throws Exception {
        readyFuture.get();
    }

    public Future<Void> getReadyFuture() {
        return readyFuture;
    }

    public boolean isClosing() {
        return closingFuture.isDone();
    }

    public void waitClosing() throws Exception {
        closingFuture.get();
    }

    public Future<Exception> getClosingFuture() {
        return closingFuture;
    }

    public boolean isClosed() {
        return closedFuture.isDone();
    }

    public Future<Exception> getClosedFuture() {
        return closedFuture;
    }

    public Exception waitClosed() throws Exception {
        // Close reason is returned as an Exception object.  An exception
        // may be thrown in fatal internal errors only.
        return closedFuture.get();
    }

    public void close(Exception reason) {
        closeRaw(reason);
    }

    public void close(String reason) {
        close(new JsonRpcException("CONNECTION_CLOSED", reason, null, null));
    }

    public void close() {
        close((Exception) null);
    }

    // Note: close reason may be set before isClosed() is true.
    public Exception getCloseReason() {
        return closeReason;
    }

    public Future<JSONObject> sendRequestAsync(String method, JSONObject params, JSONObject args) throws Exception {
        if (method == null) {
            throw new IllegalArgumentException("method must be a string");
        }
        // null 'params' is treated the same as an empty JSONObject.

        // If connection is closing or closed, return the error as a Future
        // rather than an immediate throw, because the caller cannot avoid
        // a race where connection .isClosed() == false, but on a subsequent
        // sendRequestAsync() call the connection is already closed.

        if (closingFuture.isDone() || closedFuture.isDone()) {
            Log.i(logTag, String.format("attempted to send request %s when connection closing/closed, returning error future", method));
            CompletableFutureSubset<JSONObject> result = new CompletableFutureSubset<JSONObject>();
            result.completeExceptionally(closeReason);
            return result;
        }

        String id = getJsonrpcId();
        JSONObject msg = new JSONObject();
        msg.put("jsonrpc", "2.0");
        msg.put("method", method);
        msg.put("params", params != null ? params : new JSONObject());
        msg.put("id", id);

        CompletableFutureSubset result = new CompletableFutureSubset<JSONObject>();
        boolean wasEmpty = pendingOutboundRequests.isEmpty();
        pendingOutboundRequests.put(id, result);
        writeBox(msg);
        statsOutboundRequests.bump(method);

        // If pending requests goes from 0->1, trigger an immediate
        // keepalive check to trigger a new keepalive quickly.
        keepaliveTriggerFuture.complete(null);

        return result;
    }

    public Future<JSONObject> sendRequestAsync(String method, JSONObject params) throws Exception {
        return sendRequestAsync(method, params, null);
    }

    public JSONObject sendRequestSync(String method, JSONObject params, JSONObject args) throws Exception {
        Future<JSONObject> fut = sendRequestAsync(method, params, args);
        return fut.get();
    }

    public JSONObject sendRequestSync(String method, JSONObject params) throws Exception {
        return sendRequestSync(method, params, null);
    }

    public void sendNotifySync(String method, JSONObject params, JSONObject args) throws Exception {
        if (method == null) {
            throw new IllegalArgumentException("method must be a string");
        }
        // null 'params' is treated the same as an empty JSONObject.

        if (closingFuture.isDone() || closedFuture.isDone()) {
            Log.i(logTag, String.format("attempted to send notify %s when connection closing/closed, ignoring", method));
            return;
        }

        JSONObject msg = new JSONObject();
        msg.put("jsonrpc", "2.0");
        msg.put("method", method);
        msg.put("params", params != null ? params : new JSONObject());

        writeBox(msg);
        statsOutboundNotifys.bump(method);
    }

    public void sendNotifySync(String method, JSONObject params) throws Exception {
        sendNotifySync(method, params, null);
    }

    // Get write queue length in bytes.  This is a useful rough estimate
    // for throttling; e.g. network proxy code can stop reading data from
    // internet if the queue is too long.
    public long getWriteQueueBytes() {
        synchronized (this) {
            long result = 0;
            for (byte[] s : writeQueue) {
                result += s.length;
            }
            return result;
        }
    }

    /*
     *  Misc helpers
     */

    private void setCloseReason(Exception exc) {
        if (closeReason != null) {
            //Log.v(logTag, "wanted to set close reason, but already set; ignoring", exc);
        } else if (exc == null) {
            Log.i(logTag, "wanted to set close reason, but argument was null; ignoring");
        } else {
            Log.d(logTag, "setting close reason", exc);
            closeReason = ExceptionUtil.unwrapExecutionExceptionsToException(exc);
        }
    }

    private void logStats() {
        long now = SystemClock.uptimeMillis();
        StringBuilder sb = new StringBuilder();
        sb.append("stats:");
        sb.append(String.format(" startTime=%d, readyTime=%d, closingTime=%d, closeTime=%d", statsStartedTime, statsReadyTime, statsClosingTime, statsClosedTime));
        if (statsStartedTime > 0 && statsClosedTime > 0) {
            sb.append(String.format(" (closed, duration %d seconds)", (statsClosedTime - statsStartedTime) / 1000));
        } else if (statsStartedTime > 0) {
            sb.append(String.format(" (open, duration %d seconds)", (now - statsStartedTime) / 1000));
        }
        sb.append(String.format(", bytesOut=%d, boxesOut=%d, bytesIn=%d, boxesIn=%d", statsBytesSent, statsBoxesSent, statsBytesReceived, statsBoxesReceived));
        sb.append(", outbound requests: ");
        statsOutboundRequests.formatTo(sb);
        sb.append(", outbound notifys: ");
        statsOutboundNotifys.formatTo(sb);
        sb.append(", inbound requests: ");
        statsInboundRequests.formatTo(sb);
        sb.append(", inbound notifys: ");
        statsInboundNotifys.formatTo(sb);

        Log.i(logTag, sb.toString());
    }

    private void checkLogStats() {
        long now = SystemClock.uptimeMillis();
        if (now - statsLastTime >= statsLogInterval) {
            statsLastTime = now;
            logStats();
        }
    }

    private boolean isHexDigit(byte b) {
        return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F');
    }

    private String getJsonrpcId() {
        synchronized (this) {
            return String.format("pos-%d-%d", connectionId, ++requestIdCounter);
        }
    }

    // JSONRPC Transport requires that all messages are pure ASCII, so replace
    // any non-ASCII characters with escapes.  Such characters can only appear
    // inside key or value strings, so string quoting is always appropriate.
    // Use a custom algorithm rather than an external library to minimize
    // dependencies.  Optimize for skipping large sections of ASCII data because
    // non-ASCII is rare in practice.
    private String ensureJsonAscii(String x) {
        StringBuilder sb = new StringBuilder();
        int start, end, len;

        start = 0;
        len = x.length();
        while (true) {
            // Find maximal safe ASCII range [start,end[.
            for (end = start; end < len; end++) {
                int cp = x.charAt(end);
                if (cp >= 0x80) {
                    break;
                }
            }

            // Append pure ASCII [start,end[ (may be zero length).
            sb.append(x, start, end);

            // Deal with a possible non-ASCII character, or finish.
            if (end < len) {
                sb.append(String.format("\\u%04x", ((int) x.charAt(end)) & 0xffff));
                start = end + 1;
            } else {
                break;
            }
        }

        return sb.toString();
    }

    // Android JSON binding escapes '/' as '\/' in string data.
    // which is unnecessary because '/' is unambiguous.  Un-escape
    // forward escapes; this replacement should be safe because
    // escapes may only appear in JSON strings and the unescaped
    // version is unambiguous.  It's important to ensure that the
    // backslash itself is not escaped, i.e. '\\/' must not be
    // un-escaped to '\/'.
    private String unescapeJsonForwardSlash(String x) {
        return x.replaceAll("(?<!\\)\\/", "/");
    }

    private void writeJsonrpcRequest(String method, String id, JSONObject params) throws JSONException, IOException {
        JSONObject msg = new JSONObject();
        msg.put("jsonrpc", "2.0");
        msg.put("method", method);
        msg.put("id", id);
        msg.put("params", params != null ? params : new JSONObject());

        writeBox(msg);
        statsOutboundRequests.bump(method);
    }

    private void writeJsonrpcResult(String method, String id, JSONObject result) throws JSONException, IOException {
        JSONObject msg = new JSONObject();
        msg.put("jsonrpc", "2.0");
        msg.put("response_to", method);
        msg.put("id", id);
        msg.put("result", result != null ? result : new JSONObject());

        writeBox(msg);
    }

    private void writeJsonrpcError(String method, String id, Throwable exc) throws JSONException, IOException {
        if (exc == null) {
            exc = new JsonRpcException("UNKNOWN", "unknown error (exc == null)", null, null);
        }
        JSONObject msg = new JSONObject();
        msg.put("jsonrpc", "2.0");
        msg.put("response_to", method);
        msg.put("id", id);
        msg.put("error", JsonRpcException.exceptionToErrorBox(exc));

        writeBox(msg);
    }

    private void writeRaw(byte[] data, boolean queued) throws IOException {
        if (queued) {
            if (closingFuture.isDone() || closedFuture.isDone()) {
                Log.i(logTag, "tried to writeBox() when connection closing/closed, dropping");
                return;
            }
            synchronized (this) {
                writeQueue.add(data);
                writeTriggerFuture.complete(null);
            }
        } else {
            // Non-queued writes are allowed in closing state.
            if (closedFuture.isDone()) {
                Log.i(logTag, "tried to writeBox() when connection closed, dropping");
                return;
            }
            statsBytesSent += data.length;
            statsBoxesSent++;
            Log.i(logTag, String.format("SEND (non-queued): %s", new String(data)));

            connOs.write(data);
            connOs.flush();
        }
    }

    private void writeBox(JSONObject msg, boolean queued) throws IOException {
        String msgString = ensureJsonAscii(msg.toString());
        int msgLength = msgString.length();
        String framed = String.format("%08x:%s\n", msgLength, msgString);
        byte data[] = framed.getBytes("UTF-8");
        writeRaw(data, queued);
    }

    private void writeBox(JSONObject msg) throws IOException {
        writeBox(msg, true);  // queued
    }

    // Wait future to complete (with error or success), with timeout.
    // Returns: true=future was set, false=timeout.
    private boolean waitFutureWithTimeout(Future fut, long timeout) throws Exception {
        long start = SystemClock.uptimeMillis();
        //Log.v(logTag, String.format("wait for future with timeout %d ms", timeout));
        try {
            fut.get(timeout, TimeUnit.MILLISECONDS);
            //Log.v(logTag, String.format("future completed with success after %d ms", SystemClock.uptimeMillis() - start));
            return true;
        } catch (CancellationException e) {
            // Future is done, with cancellation.
            //Log.v(logTag, String.format("future was cancelled after %d ms", SystemClock.uptimeMillis() - start), e);
            return true;
        } catch (ExecutionException e) {
            // Future is done, with error.
            //Log.v(logTag, String.format("future completed with error after %d ms", SystemClock.uptimeMillis() - start), e);
            return true;
        } catch (InterruptedException e) {
            // Don't expect interruptions, so log and re-throw.
            Log.i(logTag, String.format("unexpected thread interruption after %d ms", SystemClock.uptimeMillis() - start), e);
            throw e;
        } catch (TimeoutException e) {
            // Expected timeout.
            //Log.v(logTag, String.format("timeout after %d ms", SystemClock.uptimeMillis() - start));
            return false;
        } catch (Exception e) {
            // Unexpected exception, log and re-throw.
            Log.i(logTag, String.format("unexpected exception after %d ms", SystemClock.uptimeMillis() - start), e);
            throw e;
        }
    }

    // Wait thread to terminate with timeout.  Returns: true if thread
    // terminated within limit, false if not.
    private boolean waitThreadWithTimeout(Thread t, long timeout) {
        if (t == null) {
            return true;
        }
        long start = SystemClock.uptimeMillis();
        long deadline = start + timeout;
        while (true) {
            long remain = deadline - SystemClock.uptimeMillis();
            Log.d(logTag, "waiting for thread to finish, remain: " + remain);
            if (t.getState() == Thread.State.TERMINATED) {
                return true;
            }
            if (remain <= 0) {
                break;
            }
            try {
                t.join(remain);
            } catch (InterruptedException e) {
                Log.d(logTag, "interrupted while waiting for thread to exit, ignoring", e);
            }
        }
        return false;
    }

    /*
     *  Transport level methods
     */

    private void initInternalDispatcher() {
        internalDispatcher.registerMethod("_Keepalive", new JsonRpcInlineMethodHandler() {
            public JSONObject handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception {
                return null;
            }
        });
        internalDispatcher.registerMethod("_Error", new JsonRpcInlineMethodHandler() {
            public JSONObject handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception {
                Log.w(logTag, "peer sent an _Error notify: " + params.toString());
                return null;
            }
        });
        internalDispatcher.registerMethod("_Info", new JsonRpcInlineMethodHandler() {
            public JSONObject handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception {
                Log.i(logTag, "peer sent an _Info notify: " + params.toString());
                return null;
            }
        });
        internalDispatcher.registerMethod("_CloseReason", new JsonRpcInlineMethodHandler() {
            public JSONObject handle(JSONObject params, JsonRpcMethodExtras extras) throws Exception {
                Log.i(logTag, "peer sent a _CloseReason: " + params.toString());
                JSONObject reason = params.optJSONObject("error");
                if (reason != null) {
                    setCloseReason(JsonRpcException.errorBoxToException(reason));
                } else {
                    Log.i(logTag, "received _CloseReason, but 'error' is missing or unacceptable, ignoring");
                }
                return null;
            }
        });
    }

    /*
     *  Close handling
     */

    // For Suspend test use, not fully functional.
    public void closeStreamsRaw() {
        Log.i(logTag, "hard closing input and output streams");
        try {
            if (connOs != null) {
                connOs.close();
            }
        } catch (Exception e) {
            Log.i(logTag, "failed to close output stream, ignoring", e);
        }
        try {
            if (connIs != null) {
                connIs.close();
            }
        } catch (Exception e) {
            Log.i(logTag, "failed to close input stream, ignoring", e);
        }
    }

    private void closeRaw(Exception reason) {
        // Internal 'closeReason' sticks to first local *or* remote close
        // reason.  If close reason comes from peer, it is echoed back in
        // our own close reason.
        if (reason == null) {
            reason = new JsonRpcException("CONNECTION_CLOSED", "closed by application request", null, null);
        }
        setCloseReason(reason);

        if (closingFuture.isDone()) {
            //Log.v(logTag, "trying to close, already closed or closing");
            return;
        }

        // The close sequence happens in a separate thread so that the caller
        // never blocks.  The caller can wait for closure using waitClosed().
        Thread t = new Thread(new Runnable() {
            public void run() {
                //Log.v(logTag, "close thread started");
                runCloseThread();
                //Log.v(logTag, "close thread finished");
            }
        });
        t.start();
    }

    private void runCloseThread() {
        // If connection is not .start()ed, start it now so that we can
        // close the threads always the same way.  The threads will wait
        // for startedFuture and bail out once we set it.
        if (!startedFuture.isDone()) {
            Log.d(logTag, "close thread: start() not called, call it first");
            try {
                start();
            } catch (Exception e) {
                Log.w(logTag, "failed to start");
            }
        }

        startedFuture.completeExceptionally(closeReason);
        readyFuture.completeExceptionally(closeReason);
        closingFuture.complete(closeReason);
        // closedFuture set when close sequence is (mostly) done.
        statsClosingTime = SystemClock.uptimeMillis();
        Log.i(logTag, "connection closing: " + closeReason.toString());

        // The read thread may be blocked on a stream read without timeout.
        // Close the input stream to force it to exit quickly.
        try {
            if (connIs != null) {
                Log.d(logTag, "closing input stream");
                connIs.close();
            }
        } catch (Exception e) {
            Log.i(logTag, "failed to close input stream", e);
        }

        // The keepalive thread is potentially waiting on a trigger future or
        // the last _Keepalive sent which is tracked explicitly.  Set both
        // futures to force the thread to detect 'closing' and finish quickly.
        if (keepaliveThread != null) {
            try {
                keepaliveTriggerFuture.complete(null);
            } catch (Exception e) {
                Log.i(logTag, "failed to trigger keepalive thread", e);
            }
        }
        if (pendingKeepalive != null) {
            try {
                pendingKeepalive.complete(new JSONObject());
            } catch (Exception e) {
                Log.i(logTag, "failed to force pending keepalive to success", e);
            }
        }

        // The write thread is potentially waiting on a trigger future, set it
        // to force the write thread to detect 'closing' and finish quickly.
        // The write thread writes out the close reason.  Ideally nothing comes
        // after the _CloseReason in the output stream data.  Forcibly close the
        // output stream and drain the write queue if the writer doesn't close
        // cleanly on its own.
        try {
            writeTriggerFuture.complete(null);
        } catch (Exception e) {
            Log.i(logTag, "failed to trigger write thread", e);
        }
        if (!waitThreadWithTimeout(writeThread, THREAD_EXIT_TIMEOUT)) {
            Log.w(logTag, "write thread didn't exit within timeout");
        }
        try {
            if (writeQueue != null) {
                writeQueue.clear();
            }
        } catch (Exception e) {
            Log.i(logTag, "failed to drain write queue", e);
        }
        try {
            if (connOs != null) {
                Log.d(logTag, "closing output stream");
                connOs.close();
            }
        } catch (Exception e) {
            Log.i(logTag, "failed to close output stream", e);
        }

        // Wait for the read loop to complete cleanly.
        if (!waitThreadWithTimeout(readThread, THREAD_EXIT_TIMEOUT)) {
            Log.w(logTag, "read thread didn't exit within timeout");
        }

        // Wait for the _Keepalive thread to complete cleanly.
        if (keepaliveThread != null) {
            if (!waitThreadWithTimeout(keepaliveThread, THREAD_EXIT_TIMEOUT)) {
                Log.w(logTag, "keepalive thread didn't exit within timeout");
            }
        }

        // Finally, set close reason to closedFuture (all other lifecycle
        // futures were completed when closing started) and pending
        // requests.  Pending inbound requests will remain running, and
        // their results will be ignored.
        //Log.v(logTag, "set pending outbound request futures and closed future to close reason");
        synchronized (this) {
            for (String key : pendingOutboundRequests.keySet()) {
                CompletableFutureSubset<JSONObject> fut = pendingOutboundRequests.get(key);
                fut.completeExceptionally(closeReason);
            }
            pendingOutboundRequests.clear();
            closedFuture.complete(closeReason);

            statsClosedTime = SystemClock.uptimeMillis();
            logStats();
        }
        Log.i(logTag, "connection closed: " + closeReason.toString());

        // Normally threads are finished now, but if they aren't, track
        // them for a while and log about their status.  This happens
        // after isClosed() is already set so it won't affect the caller.
        long startThreadPoll = SystemClock.uptimeMillis();
        while (true) {
            long t = SystemClock.uptimeMillis() - startThreadPoll;
            boolean readOk = (readThread == null || readThread.getState() == Thread.State.TERMINATED);
            boolean writeOk = (writeThread == null || writeThread.getState() == Thread.State.TERMINATED);
            boolean keepaliveOk = (keepaliveThread == null || keepaliveThread.getState() == Thread.State.TERMINATED);
            if (readOk && writeOk && keepaliveOk) {
                //Log.v(logTag, "all threads finished");
                break;
            } else {
                if (t >= FINAL_THREAD_WAIT_TIMEOUT) {
                    Log.w(logTag, String.format("all threads not yet finished, waited %d ms, giving up", t));
                    break;
                } else {
                    Log.w(logTag, String.format("all threads not yet finished, waited %d ms, still waiting", t));
                }
            }
            SystemClock.sleep(1000);
        }
    }

    /*
     *  Start handling
     */

    private void startRaw() {
        if (startedFuture.isDone()) {
            throw new IllegalArgumentException("already started");
        }
        if (readThread != null || writeThread != null || keepaliveThread != null) {
            throw new InternalErrorException("readThread, writeThread, or keepaliveThread != null");
        }

        Log.i(logTag, "connection starting");
        startedFuture.complete(null);
        statsStartedTime = SystemClock.uptimeMillis();

        readThread = new Thread(new Runnable() {
            public void run() {
                //Log.v(logTag, "read thread starting");
                try {
                    runReadLoop();
                    Log.v(logTag, "read thread exited with success");
                    close(new JsonRpcException("CONNECTION_CLOSED", "read loop exited cleanly", null, null));
                } catch (Exception e) {
                    Log.i(logTag, "read thread failed", e);
                    close(e);
                }
                //Log.v(logTag, "read thread ending");
            }
        });
        readThread.start();

        writeThread = new Thread(new Runnable() {
            public void run() {
                //Log.v(logTag, "write thread starting");
                try {
                    runWriteLoop();
                    close(new JsonRpcException("CONNECTION_CLOSED", "write loop exited cleanly", null, null));
                } catch (Exception e) {
                    Log.i(logTag, "write thread failed", e);
                    close(e);
                }

                // Once the write queue has been dealt with, send
                // a _CloseReason and close the socket.
                try {
                    Exception reason = closeReason;
                    if (reason == null) {
                        // Should not happen.
                        Log.w(logTag, "close reason is null when writer thread writing _CloseReason, should not happen");
                        reason = new JsonRpcException("CONNECTION_CLOSED", "null close reason", null, null);
                    }
                    Log.d(logTag, "sending _CloseReason", reason);
                    JSONObject msg = new JSONObject();
                    msg.put("jsonrpc", "2.0");
                    msg.put("method", "_CloseReason");
                    JSONObject params = new JSONObject();
                    msg.put("params", params);
                    JSONObject error = JsonRpcException.exceptionToErrorBox(reason);
                    params.put("error", error);
                    statsOutboundNotifys.bump("_CloseReason");
                    writeBox(msg, false); // direct write, skip queue; allowed also when in closing state
                } catch (Exception e) {
                    Log.i(logTag, "failed to send _CloseReason", e);
                }

                //Log.v(logTag, "write thread ending");
            }
        });
        writeThread.start();

        if (keepaliveEnabled) {
            keepaliveThread = new Thread(new Runnable() {
                public void run() {
                    //Log.v(logTag, "keepalive thread starting");
                    try {
                        runKeepaliveLoop();
                        close(new JsonRpcException("CONNECTION_CLOSED", "keepalive loop exited cleanly", null, null));
                    } catch (Exception e) {
                        Log.i(logTag, "keepalive thread failed", e);
                        close(e);
                    }
                    //Log.v(logTag, "keepalive thread ending");
                }
            });
            keepaliveThread.start();
        }
    }

    /*
     *  Keepalive thread
     */

    private void runKeepaliveLoop() throws Exception {
        // Wait for actual start.
        startedFuture.get();

        // Wait for _Sync completion before starting keepalives.
        readyFuture.get();

        while (true) {
            long now = SystemClock.uptimeMillis();

            if (closingFuture.isDone()) {
                Log.d(logTag, "connection closing/closed, clean exit for keepalive thread");
                break;
            }

            // Send a _Keepalive with a fixed, sane timeout.  Track the
            // request so that close() can force a quick exit.  Downcast
            // for Future is safe because we know the internal type.
            Future<JSONObject> resFut = sendRequestAsync("_Keepalive", null);
            pendingKeepalive = (CompletableFutureSubset<JSONObject>)resFut;
            try {
                resFut.get(KEEPALIVE_REQUEST_TIMEOUT, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                throw new JsonRpcException("KEEPALIVE", "keepalive timeout", null, null);
            }

            // Wait before sending a new _Keepalive, with interval depending on
            // whether there are pending requests or not.  We can be woken up
            // by a trigger future, which is used when pending requests go from
            // 0->1 and we want to recheck the connection keepalive immediately.
            // It's also used to force a quick, clean exit.
            long interval = (pendingOutboundRequests.isEmpty() ? keepaliveIdleInterval : keepaliveBusyInterval);
            Log.d(logTag, String.format("keepalive wait: %d", interval));
            if (keepaliveTriggerFuture.isDone()) {
                Log.d(logTag, "refresh keepalive trigger future");
                keepaliveTriggerFuture = new CompletableFutureSubset<Void>();
            }
            if (waitFutureWithTimeout(keepaliveTriggerFuture, interval)) {
                Log.i(logTag, "keepalive triggered explicitly");
            }
        }
    }

    /*
     *  Read thread
     */

    private void readAndDiscard(long durationMillis) throws Exception {
        byte tmp[] = new byte[1024];
        long startTime = SystemClock.uptimeMillis();
        int discardedBytes = 0;
        while (true) {
            long now = SystemClock.uptimeMillis();
            //Log.v(logTag, String.format("readAndDiscard, %d of %d millis done", now - startTime, durationMillis));
            if (closingFuture.isDone()) {
                Log.d(logTag, "connection closing/closed, stop readAndDiscard");
                break;
            }
            if (now - startTime >= durationMillis) {
                //Log.v(logTag, "readAndDiscard done");
                break;
            }
            int available = connIs.available();
            //Log.v(logTag, String.format("readAndDiscard, available=%d", available));
            if (available > 0) {
                // If InputStream's .available() returns > 0, we assume
                // that this .read() will never block.  If the stream does
                // not implement .available(), it should return 0.
                int got = connIs.read(tmp);
                if (got < 0) {
                    throw new JsonRpcException("UNKNOWN", "input stream EOF while discarding", null, null);
                }
                discardedBytes += got;  // not included in stats
            }
            SystemClock.sleep(DISCARD_LOOP_DELAY);
        }
        Log.i(logTag, String.format("discarded %d initial bytes in %d ms", discardedBytes, durationMillis));
    }

    // Trial parse a length prefixed (HHHHHHHH:<...>\n) JSONRPC frame from
    // a given buffer with 'buflen' available bytes, starting at offset 'base'.
    // There are three possible outcomes:
    //   1. JsonRpcTrialParseResult: valid, complete frame.
    //   2. null: possibly valid frame, but the frame is not complete.
    //   3. throw: invalid frame.
    private JsonRpcTrialParseResult trialParseJsonRpcFrame(byte [] buf, int base, int buflen) throws Exception {
        int avail = buflen - base;

        if (avail < 9) {
            return null;
        }

        int len;
        try {
            long lenTmp = Long.parseLong(new String(buf, base, 8, "UTF-8"), 16);
            if (lenTmp < 0) {
                throw new JsonRpcParseErrorException(String.format("framing error: length is negative: %d", lenTmp));
            } else if (lenTmp > maxFrameLength) {
                throw new JsonRpcParseErrorException(String.format("framing error: frame too long: %d", lenTmp));
            }
            len = (int) lenTmp;
        } catch (NumberFormatException e) {
            throw new JsonRpcParseErrorException("framing error: cannot parse length", e);
        } catch (UnsupportedEncodingException e) {
            throw new JsonRpcParseErrorException("framing error: cannot parse length", e);
        }

        if (buf[base + 8] != ':') {
            throw new JsonRpcParseErrorException("framing error: expected colon after length");
        }

        if (avail < len + 10) {
            // No full frame received yet, continue later.  We could have a timeout for
            // incomplete frames, but there's no need because _Keepalive monitoring will
            // catch a never-completing frame automatically.
            //Log.v(logTag, "frame incomplete, continue later");
            return null;
        }

        if (buf[base + len + 9] != '\n') {
            throw new JsonRpcParseErrorException("framing error: expected newline at end of message");
        }

        // Full frame exists in the buffer, try to parse it.  org.json.JSONTokener()
        // is unfortunately very loose and allows a lot of non-standard syntax, see
        // https://developer.android.com/reference/org/json/JSONTokener.html.
        // The JSONObject constructor just calls JSONTokener internally.
        //Log.v(logTag, String.format("parsing complete frame of %d bytes", len));
        JSONObject msg;
        try {
            String jsonStr = new String(buf, base + 9, len, "UTF-8");
            String logStr = new String(buf, base, len + 9, "UTF-8");  // omit newline
            Log.i(logTag, String.format("RECV: %s", logStr));
            msg = new JSONObject(jsonStr);
        } catch (UnsupportedEncodingException e) {
            throw new JsonRpcParseErrorException("framing error: failed to parse UTF-8 encoded text", e);
        } catch (JSONException e) {
            throw new JsonRpcParseErrorException("framing error: failed to parse JSON", e);
        }

        JsonRpcTrialParseResult res = new JsonRpcTrialParseResult();
        res.msg = msg;
        res.skip = len + 10;
        return res;
    }

    private void runReadLoop() throws Exception {
        byte buf[] = new byte[readBufferSize];
        int off = 0;
        boolean scanningSync = false;
        long syncStartTime = -1;

        // Wait for actual start.
        startedFuture.get();

        // Automatic discarding of input data is useful for RFCOMM.
        if (discardEnabled) {
            Log.i(logTag, String.format("read and discard inbound data for %d ms", discardTime));
            readAndDiscard(discardTime);
        }

        // For Bluetooth RFCOMM, send a unique _Sync and hunt for a response
        // for a limited amount of time.  _Sync filler is for improving SPm20
        // RFCOMM resume behavior; SPm20 hardware loses a few bytes and may
        // corrupt a few more after a resume.
        String syncId = null;
        if (syncEnabled) {
            Log.i(logTag, String.format("send _Sync, hunt for response within %d ms", syncTimeout));
            String syncFiller = "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n";
            writeRaw(syncFiller.getBytes("UTF-8"), true);
            SystemClock.sleep(100);
            syncId = getJsonrpcId();
            writeJsonrpcRequest("_Sync", syncId, new JSONObject());
            scanningSync = true;
            syncStartTime = SystemClock.uptimeMillis();
        } else {
            statsReadyTime = SystemClock.uptimeMillis();
            readyFuture.complete(null);
        }

        // Main read loop.
        while (true) {
            // If closing, exit cleanly.
            if (closingFuture.isDone()) {
                Log.d(logTag, "connection closing/closed, clean exit for read thread");
                break;
            }

            // Check for sync timeout.
            if (scanningSync) {
                long t = SystemClock.uptimeMillis() - syncStartTime;
                Log.d(logTag, String.format("waiting for _Sync response, waited %d ms so far", t));
                if (t >= syncTimeout) {
                    throw new JsonRpcException("SYNC_FAILED", "timeout scanning for _Sync response", null, null);
                }
            }

            // Check for buffer space: we don't want to be stuck waiting for
            // a frame with no space to complete it.
            if (off < 0 || off > buf.length) {
                throw new JsonRpcInternalErrorException("internal error, invalid offset");
            }
            int space = buf.length - off;
            if (space <= 0) {
                // Frame not complete and no space to complete it.
                throw new JsonRpcParseErrorException("framing error: read buffer full, cannot parse message");
            }
            if (space > buf.length) {
                throw new JsonRpcInternalErrorException("internal error, invalid space");
            }

            // Blocking read() for more data.  If stream is broken, this will
            // (eventually) throw.  In scan mode don't block so we can time out
            // during scan.
            if (scanningSync) {
                int available = connIs.available();  // See comments in readAndDiscard()
                Log.v(logTag, String.format("reading in sync mode, off=%d, space=%d, available=%d", off, space, available));
                if (available > 0) {
                    int got = connIs.read(buf, off, space);
                    Log.v(logTag, String.format("read returned %d", got));
                    if (got < 0) {
                        throw new JsonRpcException("SYNC_FAILED", "input stream EOF while waiting for _Sync response", null, null);
                    }
                    if (got > space) {
                        throw new JsonRpcInternalErrorException("internal error, read() return value invalid");
                    }
                    statsBytesReceived += got;
                    off += got;
                } else {
                    Log.v(logTag, "no available data, sleep and retry");
                    SystemClock.sleep(250);
                }
            } else {
                //Log.v(logTag, String.format("reading, off=%d, space=%d", off, space));
                int got = connIs.read(buf, off, space);
                //Log.v(logTag, String.format("read returned %d", got));
                if (got < 0) {
                    Log.d(logTag, "input stream EOF");
                    return;
                }
                if (got > space) {
                    throw new JsonRpcInternalErrorException("internal error, read() return value invalid");
                }
                //Log.i(logTag, "RECVHEX: " + com.npay.np.android.util.Hex.encode(buf, off, got));
                statsBytesReceived += got;
                off += got;
            }

            // For _Sync mode, trial parse at every offset until we find a _Sync
            // reply.  Ignore any errors.  Note that we must scan every available
            // position because there may be a huge pending frame before a _Sync
            // reply (e.g. "12345678:..." which never completes) and we must not
            // get stuck in that pending frame.
            if (scanningSync) {
                for (int base = 0; base < off; base++) {
                    // Quick pre-check: skip silently unless at least length appears valid.
                    // This is not strictly necessary except to reduce log output from
                    // trial parse attempts.
                    int avail = off - base;
                    if (avail < 9) {
                        break;
                    }
                    boolean prefixValid = isHexDigit(buf[base + 0]) && isHexDigit(buf[base + 1]) &&
                                          isHexDigit(buf[base + 2]) && isHexDigit(buf[base + 3]) &&
                                          isHexDigit(buf[base + 4]) && isHexDigit(buf[base + 5]) &&
                                          isHexDigit(buf[base + 6]) && isHexDigit(buf[base + 7]) &&
                                          buf[base + 8] == ':';
                    if (!prefixValid) {
                        continue;
                    }
                    try {
                        Log.i(logTag, String.format("trial parsing at base offset %d/%d", base, off));

                        JsonRpcTrialParseResult trialParseResult = trialParseJsonRpcFrame(buf, base, off);
                        if (trialParseResult != null) {
                            JSONObject msg = trialParseResult.msg;
                            if (msg.optString("jsonrpc", "").equals("2.0") &&
                                msg.optString("id", "").equals(syncId) &&
                                msg.optJSONObject("result") != null) {
                                Log.i(logTag, String.format("got _Sync response at offset %d, moving to non-sync mode", base));
                                scanningSync = false;
                                statsReadyTime = SystemClock.uptimeMillis();
                                readyFuture.complete(null);

                                System.arraycopy(buf, base + trialParseResult.skip,
                                                 buf, 0, off - (base + trialParseResult.skip));
                                off = off - (base + trialParseResult.skip);
                                break;
                            }

                        }
                    } catch (Exception e) {
                        Log.i(logTag, String.format("error parsing _Sync reply at offset %d, ignoring)", base), e);
                    }
                }
            }

            // For non-_Sync mode, trial parse all completed frames.  Note that a
            // single read() may complete more than one frame and we must handle
            // them all before issuing another read() which may block indefinitely.
            if (!scanningSync) {
                while (true) {
                    //Log.v(logTag, String.format("read loop, off=%d", off));

                    int base = 0;
                    JsonRpcTrialParseResult trialParseResult = trialParseJsonRpcFrame(buf, base, off);
                    JSONObject msg = null;
                    if (trialParseResult == null) {
                        // Partial frame, keep reading.
                        break;
                    }
                    System.arraycopy(buf, base + trialParseResult.skip,
                                     buf, 0, off - (base + trialParseResult.skip));
                    off = off - (base + trialParseResult.skip);
                    msg = trialParseResult.msg;

                    // Successfully parsed a framed message, handle it.  If the handler
                    // throws, assume it's an internal error and drop the transport connection.
                    // Message processing must catch any expected errors (such as a user
                    // callback throwing).
                    //Log.v(logTag, "processing parsed message");
                    try {
                        statsBoxesReceived++;
                        processBox(msg);
                    } catch (Exception e) {
                        Log.d(logTag, "failed to process incoming frame", e);
                        throw e;
                    }
                }
            }
        }
    }

    /*
     *  Inbound message handling and method dispatch
     */

    private void processBox(JSONObject msg) throws Exception {
        Object tmp;
        String method = null;
        String id = null;
        JSONObject params = null;
        JSONObject result = null;
        JSONObject error = null;

        // Message field type and presence check.  Some of the constraints
        // in the Npay JSONRPC transport are stricter than in JSONRPC 2.0;
        // for example, params/result/error values are required to be objects
        // (not e.g. arrays) and 'id' fields are required to be strings.

        tmp = msg.opt("jsonrpc");
        if (tmp == null) {
            throw new JsonRpcInvalidRequestException("inbound message missing 'jsonrpc'");
        }
        if (!(tmp instanceof String)) {
            throw new JsonRpcInvalidRequestException("inbound message 'jsonrpc' is not a string");
        }
        if (!((String) tmp).equals("2.0")) {
            throw new JsonRpcInvalidRequestException("inbound message 'jsonrpc' is not '2.0'");
        }

        tmp = msg.opt("method");
        if (tmp != null) {
            if (tmp instanceof String) {
                method = (String) tmp;
            } else {
                throw new JsonRpcInvalidRequestException("inbound message 'method' is not a string");
            }
        }

        tmp = msg.opt("id");
        if (tmp != null) {
            if (tmp instanceof String) {
                id = (String) tmp;
            } else {
                throw new JsonRpcInvalidRequestException("inbound message 'id' is not a string");
            }
        }

        tmp = msg.opt("params");
        if (tmp != null) {
            if (tmp instanceof JSONObject) {
                params = (JSONObject) tmp;
            } else {
                throw new JsonRpcInvalidRequestException("inbound message 'params' is not an object");
            }
        }

        tmp = msg.opt("result");
        if (tmp != null) {
            if (tmp instanceof JSONObject) {
                result = (JSONObject) tmp;
            } else {
                throw new JsonRpcInvalidRequestException("inbound message 'result' is not an object");
            }
        }

        tmp = msg.opt("error");
        if (tmp != null) {
            if (tmp instanceof JSONObject) {
                error = (JSONObject) tmp;
            } else {
                throw new JsonRpcInvalidRequestException("inbound message 'error' is not an object");
            }
        }

        if (params != null) {
            if (method == null) {
                throw new JsonRpcInvalidRequestException("inbound message has 'params' but no 'method'");
            }
            if (result != null) {
                throw new JsonRpcInvalidRequestException("inbound message has both 'params' and 'result'");
            }
            if (error != null) {
                throw new JsonRpcInvalidRequestException("inbound message has both 'params' and 'error'");
            }

            // If an inbound method is already running with the requested ID,
            // drop transport because request/reply guarantees can no longer
            // be met.
            if (id != null && pendingInboundRequests.containsKey(id)) {
                Log.w(logTag, "inbound request 'id' matches an already running inbound request, fatal transport error");
                throw new JsonRpcInvalidRequestException("inbound request 'id' matches an already running inbound request");
            }

            if (id != null) {
                statsInboundRequests.bump(method);
            } else {
                statsInboundNotifys.bump(method);
            }

            // Inbound method or notify dispatch.  Use internal dispatcher for
            // transport level methods, otherwise refer to external dispatcher.
            // Inline _Keepalive handling to make keepalives as prompt as
            // possible.
            if (method.equals("_Keepalive")) {
                if (id != null) {
                    writeJsonrpcResult(method, id, new JSONObject());
                }
                return;
            }
            JsonRpcDispatcher disp = (internalDispatcher != null && internalDispatcher.hasMethod(method) ?
                                      internalDispatcher : dispatcher);
            if (disp == null || !disp.hasMethod(method)) {
                if (id != null) {
                    Log.i(logTag, String.format("unhandled method %s, sending error", method));
                    writeJsonrpcError(method, id, new JsonRpcMethodNotFoundException(String.format("method %s not supported", method)));
                } else {
                    Log.i(logTag, String.format("unhandled notify %s, ignoring", method));
                }
                return;
            }

            JSONObject args = new JSONObject();
            JsonRpcMethodExtras extras = new JsonRpcMethodExtras();
            extras.method = method;
            extras.id = id;
            extras.message = msg;
            extras.connection = this;
            JsonRpcMethodHandler handler = disp.getHandler(method);
            dispatchMethodWithHandler(method, id, params, extras, handler);
        } else if (result != null) {
            if (params != null) {
                // Cannot actually happen, as 'params' was checked above.
                throw new JsonRpcInvalidRequestException("inbound message has both 'result' and 'params'");
            }
            if (error != null) {
                throw new JsonRpcInvalidRequestException("inbound message has both 'result' and 'error'");
            }
            if (id == null) {
                throw new JsonRpcInvalidRequestException("inbound message has 'result' but no 'id'");
            }

            // Inbound success result dispatch.
            CompletableFutureSubset<JSONObject> fut = pendingOutboundRequests.get(id);
            if (fut == null) {
                Log.w(logTag, String.format("unexpected jsonrpc result message, id %s, ignoring", id));
            } else  {
                pendingOutboundRequests.remove(id);
                fut.complete(result);
            }
        } else if (error != null) {
            if (params != null) {
                // Cannot actually happen, as 'params' was checked above.
                throw new JsonRpcInvalidRequestException("inbound message has both 'error' and 'params'");
            }
            if (result != null) {
                // Cannot actually happen, as 'result' was checked above.
                throw new JsonRpcInvalidRequestException("inbound message has both 'error' and 'result'");
            }
            if (id == null) {
                throw new JsonRpcInvalidRequestException("inbound message has 'error' but no 'id'");
            }

            // Inbound error result dispatch.
            CompletableFutureSubset<JSONObject> fut = pendingOutboundRequests.get(id);
            if (fut == null) {
                Log.w(logTag, String.format("unexpected jsonrpc error message, id %s, ignoring", id));
            } else  {
                Exception exc = JsonRpcException.errorBoxToException(error);
                pendingOutboundRequests.remove(id);
                fut.completeExceptionally(exc);
            }
        } else {
            throw new JsonRpcInvalidRequestException("inbound message does not have 'params', 'result', or 'error'");
        }
    }

    // Dispatch inbound request based on the specific handler subtype.
    private void dispatchMethodWithHandler(final String method, final String id, final JSONObject params, final JsonRpcMethodExtras extras, final JsonRpcMethodHandler handler) throws Exception {
        if (handler == null) {
            Exception e = new JsonRpcMethodNotFoundException(String.format("no handler for method %s", method));
            Log.i(logTag, e.getMessage(), e);
            if (id != null) {
                try {
                    writeJsonrpcError(method, id, e);
                } catch (Exception e2) {
                    close(e2);
                }
            }
        } else if (handler instanceof JsonRpcInlineMethodHandler) {
            JsonRpcInlineMethodHandler h = (JsonRpcInlineMethodHandler) handler;
            try {
                JSONObject res = h.handle(params, extras);
                if (id != null) {
                    try {
                        writeJsonrpcResult(method, id, res);
                    } catch (Exception e2) {
                        close(e2);
                    }
                }
            } catch (Exception e) {
                Log.i(logTag, String.format("inline handler for method %s failed", method), e);
                if (id != null) {
                    try {
                        writeJsonrpcError(method, id, e);
                    } catch (Exception e2) {
                        close(e2);
                    }
                }
            }
        } else if (handler instanceof JsonRpcThreadMethodHandler) {
            final JsonRpcThreadMethodHandler h = (JsonRpcThreadMethodHandler) handler;
            Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        if (id != null) {
                            pendingInboundRequests.put(id, true);
                        }
                        JSONObject res = h.handle(params, extras);
                        if (id != null) {
                            pendingInboundRequests.remove(id);
                            try {
                                writeJsonrpcResult(method, id, res);
                            } catch (Exception e2) {
                                close(e2);
                            }
                        }
                    } catch (Exception e) {
                        Log.i(logTag, String.format("thread handler for method %s failed", method), e);
                        if (id != null) {
                            pendingInboundRequests.remove(id);
                            try {
                                writeJsonrpcError(method, id, e);
                            } catch (Exception e2) {
                                close(e2);
                            }
                        }
                    }
                }
            });
            t.start();
        } else if (handler instanceof JsonRpcFutureMethodHandler) {
            // Asynchronous result, need to wait for completion.
            // For now just launch a Thread to wait for each Future
            // individually.  For specific Future subtypes (like
            // CompletableFuture) we could avoid a Thread launch.
            final JsonRpcFutureMethodHandler h = (JsonRpcFutureMethodHandler) handler;
            Future<JSONObject> tmpFut;
            try {
                tmpFut = h.handle(params, extras);
            } catch (Exception e) {
                Log.i(logTag, String.format("future handler for method %s failed", method), e);
                if (id != null) {
                    try {
                        writeJsonrpcError(method, id, e);
                    } catch (Exception e2) {
                        close(e2);
                    }
                }
                return;
            }
            if (id == null) {
                return;
            }
            pendingInboundRequests.put(id, true);

            final Future<JSONObject> resFut = tmpFut;
            Thread t = new Thread(new Runnable() {
                public void run() {
                    try {
                        JSONObject res = resFut.get();
                        try {
                            pendingInboundRequests.remove(id);
                            writeJsonrpcResult(method, id, res);
                        } catch (Exception e2) {
                            close(e2);
                        }
                    } catch (Exception e) {
                        Log.i(logTag, String.format("future handler for method %s failed", method), e);
                        try {
                            pendingInboundRequests.remove(id);
                            writeJsonrpcError(method, id, e);
                        } catch (Exception e2) {
                            close(e2);
                        }
                    }
                }
            });
            t.start();
        } else {
            throw new JsonRpcInternalErrorException("invalid method handler subtype");
        }
    }

    /*
     *  Write thread
     */

    private void runWriteLoop() throws Exception {
        // Wait for actual start.
        startedFuture.get();

        while (true) {
            byte[] data = null;

            checkLogStats();

            synchronized (this) {
                if (closingFuture.isDone()) {
                    Log.d(logTag, "connection closing/closed and no more pending writes, write loop exiting cleanly");
                    return;
                }

                data = writeQueue.poll();
                if (writeTriggerFuture.isDone()) {
                    //Log.v(logTag, "refresh write trigger future");
                    writeTriggerFuture = new CompletableFutureSubset<Void>();
                }
            }

            if (data == null) {
                // No frame in queue, wait until trigger or sanity poll.
                waitFutureWithTimeout(writeTriggerFuture, WRITE_LOOP_SANITY_TIMEOUT);
            } else {
                // When rate limiting enabled, write and consume in small
                // pieces to handle large messages reasonably for RFCOMM.
                Log.i(logTag, String.format("SEND: %s", new String(data, "UTF-8")));
                int writeChunkSize = writeRateLimiter != null ? WRITE_CHUNK_SIZE_LIMIT : WRITE_CHUNK_SIZE_NOLIMIT;
                int off;
                for (off = 0; off < data.length;) {
                    int left = data.length - off;
                    int now = Math.min(left, writeChunkSize);
                    Log.v(logTag, String.format("writing %d (range [%d,%d[) of %d bytes", now, off, off + now, data.length));
                    if (writeRateLimiter != null) {
                        writeRateLimiter.consumeSync(now);
                    }
                    statsBytesSent += now;
                    connOs.write(data, off, now);
                    connOs.flush();
                    off += now;
                }
                statsBoxesSent += 1;
            }
        }
    }
}
