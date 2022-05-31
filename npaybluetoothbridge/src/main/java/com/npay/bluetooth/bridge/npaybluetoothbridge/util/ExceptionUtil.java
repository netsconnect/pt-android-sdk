/*
 *  Exception / Throwable handling utilities.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.util;

import java.util.concurrent.ExecutionException;

public class ExceptionUtil {
    public static Throwable unwrapExecutionExceptions(Throwable e) {
        if (e == null) {
            return null;
        }
        while (e instanceof ExecutionException) {
            Throwable t = e.getCause();
            if (t == null) {
                // If ExecutionException's cause is null
                // (which should not happen), terminate
                // with non-null ExecutionException.
                break;
            }
            e = t;
        }
        return e;
    }

    public static Throwable unwrapExecutionExceptionsToThrowable(Throwable e) {
        return unwrapExecutionExceptions(e);
    }

    public static Exception unwrapExecutionExceptionsToException(Throwable e) {
        Throwable t = unwrapExecutionExceptions(e);
        if (t instanceof Exception) {
            return (Exception) t;
        } else {
            // If final cause is not an Exception, wrap it in a
            // single ExecutionException.
            return new ExecutionException("future set to error", t);
        }
    }
}
