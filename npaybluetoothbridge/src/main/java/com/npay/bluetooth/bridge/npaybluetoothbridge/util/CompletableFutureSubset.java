/*
 *  Simple subset of CompletableFuture which is only available since
 *  Android Nougat (API level 24).
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class CompletableFutureSubset<V> implements Future {
    private final static String logTag = "CompletableFutureSubset";
    private final Semaphore sem = new Semaphore(1);
    private boolean valueSet = false;
    private boolean excSet = false;
    private V value = null;
    private Throwable exc = null;

    public CompletableFutureSubset() {
        try {
            // Acquired on creation, released (exactly once) when future
            // is set.
            sem.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException("failed to initialize CompletableFutureSubset", e);
        }
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        // Doesn't support cancelling.
        return false;
    }

    public V get() throws InterruptedException, ExecutionException {
        /* // Possible fast path, not really necessary.
           synchronized (this) {
            if (valueSet) {
                //Log.v(logTag, "immediate success result for get()");
                return value;
            } else if (excSet) {
                //Log.v(logTag, "immediate error result for get()");
                throw new ExecutionException("future set to error", exc);
            }
           }
         */

        // acquire() may throw InterruptedException without acquiring;
        // it will propagate out to the caller.
        //Log.v(logTag, "wait for future completion");
        sem.acquire();
        sem.release();
        //Log.v(logTag, "future completed");

        synchronized (this) {
            if (valueSet) {
                //Log.v(logTag, "delayed success result for get()");
                return value;
            } else if (excSet) {
                //Log.v(logTag, "delayed error result for get()");
                throw new ExecutionException("future set to error", exc);
            } else {
                throw new RuntimeException("neither value nor exception set, should not happen");
            }
        }
    }

    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long millis = unit.toMillis(timeout);

        /* // Possible fast path, not really necessary.
           synchronized (this) {
            if (valueSet) {
                //Log.v(logTag, "immediate success result for get() with timeout");
                return value;
            } else if (excSet) {
                //Log.v(logTag, "immediate error result for get() with timeout");
                throw new ExecutionException("future set to error", exc);
            }
           }
         */

        // tryAcquire() may throw InterruptedException without acquiring;
        // it will propagate out to the caller.
        //Log.v(logTag, String.format("wait for future completion with timeout %d ms", millis));
        if (!sem.tryAcquire(1, millis, TimeUnit.MILLISECONDS)) {
            //Log.v(logTag, String.format("timeout waiting for future completion with timeout %d ms", millis));
            throw new TimeoutException(String.format("failed to get() CompletableFutureSubset within %d ms", millis));
        }
        sem.release();
        //Log.v(logTag, "future completed");

        synchronized (this) {
            if (valueSet) {
                //Log.v(logTag, "delayed success result for get() with timeout");
                return value;
            } else if (excSet) {
                //Log.v(logTag, "delayed error result for get() with timeout");
                throw new ExecutionException("future set to error", exc);
            } else {
                throw new RuntimeException("neither value nor exception set, should not happen");
            }
        }
    }

    public boolean isCancelled() {
        // Doesn't support cancelling.
        return false;
    }

    public boolean isDone() {
        synchronized (this) {
            return valueSet || excSet;
        }
    }

    public boolean complete(V value) {
        synchronized (this) {
            if (valueSet && excSet) {
                throw new RuntimeException("internal error, both value and exception set, should not happen");
            }
            if (valueSet || excSet) {
                //Log.v(logTag, "complete(), value already set, ignoring");
                return false;
            }
            //Log.v(logTag, "complete(), setting value");
            valueSet = true;
            this.value = value;
            sem.release();
            return true;
        }
    }

    public boolean completeExceptionally(Throwable exc) {
        synchronized (this) {
            if (valueSet && excSet) {
                throw new RuntimeException("internal error, both value and exception set, should not happen");
            }
            if (valueSet || excSet) {
                //Log.v(logTag, "completeExceptionally(), value already set, ignoring");
                return false;
            }
            //Log.v(logTag, "completeExceptionally(), setting value");
            excSet = true;
            this.exc = exc;
            sem.release();
            return true;
        }
    }
}
