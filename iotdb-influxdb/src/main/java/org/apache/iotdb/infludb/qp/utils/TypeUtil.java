package org.apache.iotdb.infludb.qp.utils;

public class TypeUtil {
    public static boolean checkDecimal(Object object) {
        return object instanceof Number;
    }
}
