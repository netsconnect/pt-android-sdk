/*
 *  Hex encode/decode, for debug logs.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.util;

public final class Hex {
    final private static char[] nybbles = "0123456789abcdef".toCharArray();

    public static final String encode(byte[] data, int off, int len) {
        char res[] = new char[len * 2];
        for (int i = 0; i < len; i++) {
            int x = data[off + i] & 0xff;
            res[i * 2] = nybbles[x >>> 4];
            res[i * 2 + 1] = nybbles[x & 0x0f];
        }
        return new String(res);
    }

    public static final String encode(byte[] data) {
        return encode(data, 0, data.length);
    }
}
