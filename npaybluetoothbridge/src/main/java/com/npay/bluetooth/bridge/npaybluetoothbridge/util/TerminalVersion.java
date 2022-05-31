/*
 *  Helpers to compare payment terminal version numbers (e.g. "18.7.0") and
 *  for specific feature checks.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.util;

public class TerminalVersion {
    private static final String logTag = "TerminalVersion";

    public static int[] breakIntoComponents(String version) {
        String parts[] = version.split("\\.");
        if (parts.length != 3) {
            throw new IllegalArgumentException("version number format invalid");
        }
        int res[] = new int[3];
        for (int i = 0; i < 3; i++) {
            res[i] = Integer.valueOf(parts[i]);
        }
        return res;
    }

    public static boolean validate(String version) {
        try {
            int ignored[] = breakIntoComponents(version);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean greaterOrEqual(String versionA, String versionB) {
        int A[] = breakIntoComponents(versionA);
        int B[] = breakIntoComponents(versionB);
        for (int i = 0; i < 3; i++) {
            if (A[i] > B[i]) {
                return true;
            }
            if (A[i] < B[i]) {
                return false;
            }
        }
        return true;  // equal
    }

    public static boolean equal(String versionA, String versionB) {
        return greaterOrEqual(versionA, versionB) && greaterOrEqual(versionB, versionA);
    }

    // Some manual development builds may have version "0.0.0".  These should
    // never appear in integrator system testing or production environments
    // however.  For feature tests, assume all new features are supported in
    // unversioned dev builds.
    public static boolean isUnversionedDevBuild(String version) {
        return version.equals("0.0.0");
    }

    public static boolean supportsStatusEvent(String version) {
        return greaterOrEqual(version, "18.2.0") || isUnversionedDevBuild(version);
    }
}
