/*
 *  Representation of JSONRPC-originated exceptions and mapping between
 *  Exceptions and JSONRPC error boxes.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.jsonrpc;

import android.util.Log;


import com.npay.bluetooth.bridge.npaybluetoothbridge.util.ExceptionUtil;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;

public class JsonRpcException extends Exception {
    private static final String logTag = "JsonRpcException";
    private static final int MAX_CODE_LENGTH = 256;
    private static final int MAX_MESSAGE_LENGTH = 1024;
    private static final int MAX_DETAILS_LENGTH = 8192;

    private String code = null;
    private String details = null;

    public JsonRpcException(String code, String message, String details, Throwable cause) {
        // Fill in (sanitized) message using super constructor.
        super(message != null ? sanitizeMessage(message) : "", cause);  // null cause is permitted

        // Fill in (sanitized) code and message separately.  Note that for default
        // 'details' code and message are intentionally defaulted but *not* sanitized,
        // because details will go through a sanitization step for the final result.
        message = (message != null ? message : "");
        code = (code != null ? code : "UNKNOWN");
        details = (details != null ? details : formatJsonRpcDefaultDetailsShort(this, code, message));
        this.code = sanitizeCode(code);
        this.details = sanitizeDetails(details);
    }

    public String getCode() {
        return code;
    }

    // getMessage() is inherited.

    public String getDetails() {
        return details;
    }

    private static String clipString(String x, int limit) {
        if (x.length() > limit) {
            return x.substring(0, limit);
        }
        return x;
    }

    private static String sanitizeCode(String code) {
        return clipString(code, MAX_CODE_LENGTH);
    }

    private static String sanitizeMessage(String message) {
        return clipString(message, MAX_MESSAGE_LENGTH);
    }

    private static String sanitizeDetails(String details) {
        return clipString(details, MAX_DETAILS_LENGTH);
    }

    // Get a formatted traceback with unsuppressed cause chain.
    // Assume caller has unwrapped ExecutionExceptions.
    public static String formatJsonRpcDefaultDetailsFull(Throwable t, String code, String message) {
        try {
            StringBuilder sb = new StringBuilder();

            boolean first = true;
            for (; t != null; t = t.getCause()) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                PrintWriter pw = new PrintWriter(baos);

                pw.print(t.getClass().getName());
                pw.print(": ");
                pw.print(t.getMessage());
                pw.print("\n");
                for (StackTraceElement e : t.getStackTrace()) {
                    pw.print("\t");
                    pw.print(e.toString());
                    pw.print("\n");
                }

                pw.flush();

                // Stack trace will contain a trailing newline which we don't want.
                String trace = new String(baos.toByteArray(), "UTF-8");
                trace = trace.trim();  // no whitespace at beginning, so same as "trim right"

                if (first) {
                    first = false;
                } else {
                    sb.append("\nCaused by: ");
                }

                sb.append(trace);
            }

            // No trailing newline for details.
            return String.format("%s: %s\n%s", code, message, sb.toString());
        } catch (Exception e) {
            Log.w(logTag, "failed to format stack trace for error, ignoring", e);
            return String.format("%s: %s", code, message);
        }
    }

    // Get a formatted traceback using .printStackTrace() on a Throwable.
    // On Android .printStackTrace() provides a reasonable (though suppressed)
    // cause chain without the need to walk the chain manually.  Assume
    // caller has unwrapped ExecutionExceptions.
    public static String formatJsonRpcDefaultDetailsShort(Throwable t, String code, String message) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            StringBuilder sb = new StringBuilder();

            PrintWriter pw = new PrintWriter(baos);
            t.printStackTrace(pw);
            pw.flush();

            // Stack trace will contain a trailing newline which we don't want.
            String trace = new String(baos.toByteArray(), "UTF-8");
            trace = trace.trim();  // no whitespace at beginning, so same as "trim right"
            return String.format("%s: %s\n%s", code, message, trace);
        } catch (Exception e) {
            Log.w(logTag, "failed to format stack trace for error, ignoring", e);
            return String.format("%s: %s", code, message);
        }
    }

    // Map a JSONRPC numeric error code to a string code.
    public static String jsonCodeToStringCode(long jsonCode) {
        if ((long) (int) jsonCode != jsonCode) {
            return "UNKNOWN";
        }
        switch ((int) jsonCode) {
        case -32700: return "JSONRPC_PARSE_ERROR";
        case -32600: return "JSONRPC_INVALID_REQUEST";
        case -32601: return "JSONRPC_METHOD_NOT_FOUND";
        case -32602: return "JSONRPC_INVALID_PARAMS";
        case -32603: return "INTERNAL_ERROR";
        case -32000: return "KEEPALIVE";
        }
        return "UNKNOWN";
    }

    // Map a string code to JSONRPC numeric error code.
    public static long stringCodeToJsonCode(String stringCode) {
        if (stringCode.equals("JSONRPC_PARSE_ERROR")) {
            return -32700;
        }
        if (stringCode.equals("JSONRPC_INVALID_REQUEST")) {
            return -32600;
        }
        if (stringCode.equals("JSONRPC_METHOD_NOT_FOUND")) {
            return -32601;
        }
        if (stringCode.equals("JSONRPC_INVALID_PARAMS")) {
            return -32602;
        }
        if (stringCode.equals("INTERNAL_ERROR")) {
            return -32603;
        }
        if (stringCode.equals("KEEPALIVE")) {
            return -32000;
        }
        return 1;
    }

    // Convert a JSONRPC 'error' box to an Exception object.
    public static Exception errorBoxToException(JSONObject error) {
        Object tmp;
        long jsonCode = 1;
        String stringCode = null;
        String details = null;
        String message = null;

        message = error.optString("message", "");
        JSONObject data = error.optJSONObject("data");
        if (data != null) {
            stringCode = data.optString("string_code");
            details = data.optString("details");
        }
        jsonCode = error.optInt("code", 1);

        if (stringCode == null) {
            stringCode = jsonCodeToStringCode(jsonCode);
        }
        if (details == null) {
            details = String.format("%s: %s", stringCode, message);
        }

        JsonRpcException exc;
        if (stringCode.equals("JSONRPC_PARSE_ERROR")) {
            exc = new JsonRpcParseErrorException(stringCode, message, details, null);
        } else if (stringCode.equals("JSONRPC_INVALID_REQUEST")) {
            exc = new JsonRpcInvalidRequestException(stringCode, message, details, null);
        } else if (stringCode.equals("JSONRPC_METHOD_NOT_FOUND")) {
            exc = new JsonRpcMethodNotFoundException(stringCode, message, details, null);
        } else if (stringCode.equals("JSONRPC_INVALID_PARAMS")) {
            exc = new JsonRpcInvalidParamsException(stringCode, message, details, null);
        } else if (stringCode.equals("INTERNAL_ERROR")) {
            exc = new JsonRpcInternalErrorException(stringCode, message, details, null);
        } else if (stringCode.equals("KEEPALIVE")) {
            exc = new JsonRpcKeepaliveException(stringCode, message, details, null);
        } else {
            exc = new JsonRpcException(stringCode, message, details, null);
        }

        return exc;
    }

    // Convert an arbitrary Exception to a JSONRPC 'error' box.
    public static JSONObject exceptionToErrorBox(Throwable exc) throws JSONException {
        long jsonCode = 1;
        String stringCode = "UNKNOWN";
        String message = exc.getMessage();
        String details;

        exc = ExceptionUtil.unwrapExecutionExceptionsToThrowable(exc);

        // Round trip exceptions coming from JSONRPC cleanly.  For other exceptions
        // use JsonRpcException() constructor which guarantees sanitized and clipped
        // results that won't exceed message size limits.
        JsonRpcException jsonExc;
        if (exc instanceof JsonRpcException) {
            jsonExc = (JsonRpcException) exc;
        } else {
            String tmpCode = "UNKNOWN";
            String tmpMessage = exc.getMessage();
            jsonExc = new JsonRpcException(tmpCode,
                                           tmpMessage,
                                           JsonRpcException.formatJsonRpcDefaultDetailsShort(exc, tmpCode, tmpMessage),
                                           null);
        }
        stringCode = jsonExc.getCode();
        message = jsonExc.getMessage();
        details = jsonExc.getDetails();
        jsonCode = stringCodeToJsonCode(stringCode);

        JSONObject error = new JSONObject();
        error.put("code", jsonCode);
        error.put("message", message);
        JSONObject data = new JSONObject();
        error.put("data", data);
        data.put("string_code", stringCode);
        data.put("details", details);
        return error;
    }
}
