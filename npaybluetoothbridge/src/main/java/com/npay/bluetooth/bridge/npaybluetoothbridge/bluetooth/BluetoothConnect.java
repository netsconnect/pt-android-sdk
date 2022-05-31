/*
 *  Helper class to obtain a Bluetooth RFCOMM connection.
 *
 *  Initially intended specifically for Spire SPm20.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.bluetooth;

import android.bluetooth.BluetoothAdapter;
import android.bluetooth.BluetoothClass;
import android.bluetooth.BluetoothDevice;
import android.bluetooth.BluetoothSocket;
import android.os.ParcelUuid;
import android.util.Log;

import com.npay.bluetooth.bridge.npaybluetoothbridge.util.CompletableFutureSubset;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class BluetoothConnect {
    private static final String logTag = "BluetoothConnect";
    private static final String BASE_UUID = "00000000-0000-1000-8000-00805F9B34FB";
    private static final String SPP_UUID = "00001101-0000-1000-8000-00805F9B34FB";
    private BluetoothAdapter btAdapter = null;
    private FutureTask<BluetoothSocket> currTask = null;

    public BluetoothConnect() {
        BluetoothAdapter adapter = BluetoothAdapter.getDefaultAdapter();
        if (adapter == null) {
            Log.w(logTag, "device doesn't support Bluetooth");
            throw new RuntimeException("failed to BluetoothAdapter.getDefaultAdapter(), device doesn't support Bluetooth");
        }
        btAdapter = adapter;
    }

    // Format a BluetoothDevice into a useful log string.
    private static String formatDevice(BluetoothDevice btDevice) {
        String devAddress = btDevice.getAddress();
        BluetoothClass devClass = btDevice.getBluetoothClass();
        int devBondState = btDevice.getBondState();
        String devName = btDevice.getName();
        //int devType = btDevice.getType();  // Avoid, requires API level 18.

        return String.format("%s (MAC %s); class=%d, bondState=%d",
                             devName, devAddress, devClass.getDeviceClass(), devBondState);
    }

    // Get a list of available Bluetooth devices.  A device is considered
    // available if it is paired and likely to be a npay device (such as
    // Spire SPm20).  Device list is sorted by ascending Bluetooth name so that
    // the result is deterministic when multiple potential devices are found.
    // The device does not need to be connected to be considered available.
    public BluetoothDevice[] getAvailableDevices() throws Exception {
        ArrayList<BluetoothDevice> res = new ArrayList<BluetoothDevice>();
        Set<BluetoothDevice> pairedDevices = btAdapter.getBondedDevices();

        Log.i(logTag, "looking for available devices");
        for (BluetoothDevice dev : pairedDevices) {
            if (dev.getName().startsWith("SP:")) {
                Log.i(logTag, "potential device: " + formatDevice(dev));
                res.add(dev);
            } else {
                Log.i(logTag, "skipped device: " + formatDevice(dev));
            }
        }

        Collections.sort(res, new Comparator<BluetoothDevice>() {
            public int compare(BluetoothDevice dev1, BluetoothDevice dev2){
                return dev1.getName().compareTo(dev2.getName());
            }
        });
        /*
           for (BluetoothDevice dev : res) {
            Log.i(logTag, "sorted: " + dev.getName());
           }
         */

        return res.toArray(new BluetoothDevice[0]);
    }

    public Future<BluetoothSocket> connect(String bluetoothMac) {
        // Android Bluetooth seems very unreliable if multiple active connection
        // attempts happen at the same time so prevent it.
        if (currTask != null && !currTask.isDone()) {
            CompletableFutureSubset<BluetoothSocket> fut = new CompletableFutureSubset<BluetoothSocket>();
            fut.completeExceptionally(new IllegalStateException("attempt to connect bluetooth while previous connection active"));
            return fut;
        }

        final String finalBluetoothMac = bluetoothMac;
        FutureTask<BluetoothSocket> task = new FutureTask<BluetoothSocket>(new Callable<BluetoothSocket>() {
            public BluetoothSocket call() throws Exception {
                if (finalBluetoothMac == null) {
                    throw new IllegalArgumentException("null bluetoothMac");
                }
                Log.i(logTag, String.format("connecting bluetooth rfcomm for mac %s", finalBluetoothMac));

                if (btAdapter.isDiscovering()) {
                    Log.w(logTag, "adapter is in discovery mode, this may make RFCOMM connections unreliable");
                }
                if (!btAdapter.isEnabled()) {
                    Log.w(logTag, "bluetooth is disabled, enable it forcibly");
                    boolean ret = btAdapter.enable();
                    Log.i(logTag, String.format("enable() -> %b", ret));
                }

                Log.d(logTag, String.format("get device for mac %s", finalBluetoothMac));
                BluetoothDevice btDevice = btAdapter.getRemoteDevice(finalBluetoothMac);
                Log.d(logTag, "got device");

                // For SPm20:
                // I/BluetoothConnect: UUID 0: 00001101-0000-1000-8000-00805f9b34fb
                // I/BluetoothConnect: UUID 1: 00000000-0000-1000-8000-00805f9b34fb
                // I/BluetoothConnect: UUID 2: 00000000-0000-1000-8000-00805f9b34fb
                // I/BluetoothConnect: UUID 3: ffcacade-afde-cade-defa-cade00000000

                String devAddress = btDevice.getAddress();
                Log.i(logTag, "got device: " + formatDevice(btDevice));

                ParcelUuid uuids[] = btDevice.getUuids();
                if (uuids == null) {
                    throw new RuntimeException(String.format("no UUIDs found for device %s", devAddress));
                }
                Log.d(logTag, String.format("found %d UUIDs for device %s", uuids.length, devAddress));
                UUID wantUuid = UUID.fromString(SPP_UUID);
                boolean foundUuid = false;
                for (int i = 0; i < uuids.length; i++) {
                    Log.i(logTag, String.format("  UUID %d: %s", i, uuids[i].toString()));
                    if (!foundUuid && uuids[i].getUuid().compareTo(wantUuid) == 0) {
                        foundUuid = true;
                    }
                }
                if (!foundUuid) {
                    throw new RuntimeException(String.format("could not find RFCOMM UUID %s", wantUuid.toString()));
                }

                // Calling connect() may cause the following log:
                //   W/BluetoothAdapter: getBluetoothService() called with no BluetoothManagerCallback
                //
                // This should be harmless.  Note that it is critical that only one
                // connection attempt is made at a time (to the target device); otherwise
                // all of the concurrent attempts may consistently fail.

                Log.i(logTag, String.format("connect to rfcomm using UUID %s", wantUuid.toString()));
                //BluetoothSocket btSocket = btDevice.createRfcommSocketToServiceRecord(wantUuid);
                BluetoothSocket btSocket = btDevice.createInsecureRfcommSocketToServiceRecord(wantUuid);
                //Log.i(logTag, String.format("BluetoothSocket connection type: %d", btSocket.getConnectionType()));  // Avoid, requires API level 23.
                Log.i(logTag, String.format("isConnected: %b", btSocket.isConnected()));
                btSocket.connect();
                Log.i(logTag, String.format("connect returned, isConnected: %b", btSocket.isConnected()));

                // .connect() is synchronous so this should not happen.
                if (!btSocket.isConnected()) {
                    throw new RuntimeException("internal error, expected bluetooth socket to be connected");
                }

                InputStream btIs = btSocket.getInputStream();
                OutputStream btOs = btSocket.getOutputStream();
                if (btIs == null || btOs == null) {
                    throw new RuntimeException("internal error, bluetooth input or output stream is null");
                }

                Log.i(logTag, String.format("successful bluetooth connection to %s, service UUID %s", devAddress, wantUuid));
                return btSocket;
            }
        });
        currTask = task;
        task.run();
        return task;
    }
}
