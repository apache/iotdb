package org.apache.iotdb.tool.ui.util;

public class StringUtils {

    public static boolean equels(String a, String b) {
        if(a == null && b == null) {
            return true;
        }
        if(a != null && b != null) {
            return a.trim().equals(b.trim());
        } else {
            return false;
        }
    }
}
