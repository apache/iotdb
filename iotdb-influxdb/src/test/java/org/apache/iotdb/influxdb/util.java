package org.apache.iotdb.influxdb;

public class util {
    public static String generateWord(int index) {
        if (index == 0) {
            return "A";
        }
        StringBuilder result = new StringBuilder();
        while (index > 0) {
            result.append((char) ((index % 26) + 'A'));
            index /= 26;

        }
        return result.toString();
    }
}
