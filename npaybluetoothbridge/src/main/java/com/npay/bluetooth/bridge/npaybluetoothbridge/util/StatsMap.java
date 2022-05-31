/*
 *  String-to-counter map for stats collection.
 */

package com.npay.bluetooth.bridge.npaybluetoothbridge.util;

import java.util.Arrays;
import java.util.HashMap;

public class StatsMap {
    private HashMap<String, Long> map = new HashMap<String, Long>();

    public StatsMap() {
    }

    public void bump(String key) {
        if (key == null) {
            return;
        }
        if (map.containsKey(key)) {
            long prev = map.get(key);
            map.put(key, prev + 1);
        } else {
            map.put(key, 1L);
        }
    }

    public void formatTo(StringBuilder sb) {
        sb.append("{");
        String keys[] = map.keySet().toArray(new String[0]);
        Arrays.sort(keys);
        boolean first = true;
        for (String key : keys) {
            if (first) {
                first = false;
                sb.append(" ");
            } else {
                sb.append(", ");
            }
            sb.append(String.format("%s:%d", key, map.get(key)));
        }
        if (!first) {
            sb.append(" ");
        }
        sb.append("}");
    }
}
