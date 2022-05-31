/*
 *  Internal error which happened inside the SDK.  May indicate a bug or
 *  violation of API requirements.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.util;

public class InternalErrorException extends RuntimeException {
    public InternalErrorException() {
        super();
    }

    public InternalErrorException(String message) {
        super(message);
    }

    public InternalErrorException(String message, Throwable cause) {
        super(message, cause);
    }

    /* Disabled, requires API level 24.
       public InternalErrorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
       }
     */

    public InternalErrorException(Throwable cause) {
        super(cause);
    }
}
