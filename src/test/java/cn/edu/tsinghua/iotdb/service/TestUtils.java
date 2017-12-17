package cn.edu.tsinghua.iotdb.service;


public class TestUtils {

    static boolean testFlag = !true;

    static String count(String path) {
        return String.format("count(%s)", path);
    }

    static String max_time(String path) {
        return String.format("max_time(%s)", path);
    }

    static String min_time(String path) {
        return String.format("min_time(%s)", path);
    }

    static String max_value(String path) {
        return String.format("max_value(%s)", path);
    }

    static String min_value(String path) {
        return String.format("min_value(%s)", path);
    }
}
