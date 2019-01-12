package org.apache.iotdb.db.postback.utils;

/**
 * Created by stefanie on 07/08/2017.
 */

public class Utils {

    public static String getType(String properties) {
        return properties.split(",")[0];
    }

    public static String getEncode(String properties) {
        return properties.split(",")[1];
    }

    public static String getPath(String timeseries) {
        int lastPointIndex = timeseries.lastIndexOf(".");
        return timeseries.substring(0, lastPointIndex);
    }

    public static String getSensor(String timeseries) {
        int lastPointIndex = timeseries.lastIndexOf(".");
        return timeseries.substring(lastPointIndex + 1);
    }

    public static void main(String[] argc) {

        String test2 = "root.excavator.Beijing.d1.s1";
        System.out.println(getPath(test2) + " " + getSensor(test2));

    }
}
