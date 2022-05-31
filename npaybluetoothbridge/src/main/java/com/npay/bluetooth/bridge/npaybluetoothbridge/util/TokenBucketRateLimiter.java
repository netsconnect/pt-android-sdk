/*
 *  Token bucket rate limiter.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.util;

import android.os.SystemClock;
import android.util.Log;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class TokenBucketRateLimiter implements RateLimiter {
    private static final long statsInterval = 300 * 1000;
    private String logTag = "TokenBucketRateLimiter";
    private long lastStats = 0;
    private long lastUptime = 0;
    private long lastCount = 0;
    private long maxTokens = 0;        // tokens
    private long tokensPerSecond = 0;  // tokens/second
    private long statsConsumed = 0;    // tokens consumed since last stats log write

    public TokenBucketRateLimiter(String name, long maxTokens, long tokensPerSecond) {
        if (maxTokens <= 0) {
            throw new IllegalArgumentException(String.format("invalid maxTokens %d", maxTokens));
        }
        if (tokensPerSecond <= 0) {
            throw new IllegalArgumentException(String.format("invalid tokensPerSecond %d", tokensPerSecond));
        }
        long now = getMillis();
        this.logTag = String.format("TokenBucketRateLimiter(%s)", name);
        this.lastStats = 0;  // force immediate stats logging on first attempt
        this.lastUptime = now;
        this.lastCount = maxTokens;  // initialize to maximum capacity
        this.maxTokens = maxTokens;
        this.tokensPerSecond = tokensPerSecond;
        this.statsConsumed = 0;
        Log.i(this.logTag, String.format("created rate limiter, maxTokens=%d, tokensPerSecond=%d", this.maxTokens, this.tokensPerSecond));
    }

    // Uptime provider.
    private long getMillis() {
        return SystemClock.uptimeMillis();
    }

    // Update current token state based on the difference between last update
    // time and current time.
    private void updateCurrent() {
        long now = getMillis();
        long diff = now - lastUptime;
        long tokenAdd = diff * tokensPerSecond / 1000;
        long timeAdd = tokenAdd * 1000 / tokensPerSecond;
        long newTokens = lastCount + tokenAdd;
        if (newTokens > maxTokens) {
            lastUptime = now;
            lastCount = maxTokens;
        } else {
            lastUptime += timeAdd;
            lastCount = newTokens;
        }
    }

    private void checkStatsLog() {
        long now = getMillis();
        if (now - lastStats >= statsInterval) {
            Log.i(logTag, String.format("stats: %d tokens consumed in last interval, %d / second (tokensPerSecond %d / second)",
                                        statsConsumed,
                                        statsConsumed * 1000 / statsInterval,
                                        tokensPerSecond));
            lastStats = now;
            statsConsumed = 0;
        }
    }

    private boolean tryConsumeRaw(long count) {
        // 'synchronized' is not strictly necessary because the only current
        // call site is itself synchronized.
        synchronized (this) {
            checkStatsLog();
            updateCurrent();
            if (lastCount >= count) {
                lastCount -= count;
                statsConsumed += count;
                return true;
            } else {
                return false;
            }
        }
    }

    // Consume 'count' tokens, blocking until complete.  Token count may be
    // higher than maxTokens; the requested number of tokens is consumed in
    // smaller pieces if necessary.  There are no explicit fairness guarantees.
    public void consumeSync(long count) {
        if (count < 0) {
            throw new RuntimeException(String.format("invalid requested token count: %d, count", count));
        }
        long chunk = Math.max(maxTokens / 2, 1);
        long left = count;
        while (left > 0) {
            long now = Math.min(left, chunk);
            long sleepTime = 0;  // dummy default, overwritten if needed
            synchronized (this) {
                if (tryConsumeRaw(now)) {
                    left -= now;
                } else {
                    sleepTime = (now - lastCount) * 1000 / tokensPerSecond + 1;
                    Log.d(logTag, String.format("need to wait %d ms, requested %d, current %d (total left %d, original requested %d)",
                                                sleepTime, now, lastCount, left, count));
                }
            }
            if (sleepTime > 0) {
                SystemClock.sleep(sleepTime);
            }
        }
    }

    // Future variant of consumeSync().
    public Future<Void> consumeAsync(long count) {
        final long finalCount = count;
        FutureTask<Void> task = new FutureTask<Void>(new Callable<Void>() {
            public Void call() throws Exception {
                consumeSync(finalCount);
                return null;
            }
        });
        task.run();
        return task;
    }
}
