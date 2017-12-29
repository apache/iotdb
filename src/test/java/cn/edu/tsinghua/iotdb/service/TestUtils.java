package cn.edu.tsinghua.iotdb.service;


public class TestUtils {

    public static boolean testFlag = true;

    public static String first(String path) {
        return String.format("first(%s)", path);
    }

    public static String sum(String path) {
        return String.format("sum(%s)", path);
    }

    public static String mean(String path) {
        return String.format("mean(%s)", path);
    }

    public static String count(String path) {
        return String.format("count(%s)", path);
    }

    public static String max_time(String path) {
        return String.format("max_time(%s)", path);
    }

    public static String min_time(String path) {
        return String.format("min_time(%s)", path);
    }

    public static String max_value(String path) {
        return String.format("max_value(%s)", path);
    }

    public static String min_value(String path) {
        return String.format("min_value(%s)", path);
    }
}
