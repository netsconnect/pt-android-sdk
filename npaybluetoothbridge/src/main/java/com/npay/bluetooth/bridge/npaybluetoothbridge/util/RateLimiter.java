/*
 *  Rate limiter interface.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.util;

import java.util.concurrent.Future;

public interface RateLimiter {
    void consumeSync(long count);
    Future<Void> consumeAsync(long count);
}
