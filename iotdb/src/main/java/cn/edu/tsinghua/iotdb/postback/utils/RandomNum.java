package cn.edu.tsinghua.iotdb.postback.utils;

import java.util.Random;
import java.time.LocalDate;

/**
 * Created by stefanie on 26/07/2017.
 */
public class RandomNum {

    private static Random random = new Random();

    public static long getRandomLong(long min, long max) {
        return Math.abs(random.nextLong()) % (max - min + 1) + min;
    }

    public static int getRandomInt(int min, int max) {
        // return Math.abs(random.nextInt() % (max-min+1) + min);
        return (random.nextInt(10000) % (max - min) + min);
    }

    public static float getRandomFloat(float min, float max) {

        Random random = new Random();
        return (random.nextFloat() * (max - min) + min);
    }

    public static int getAbnormalData(int frequency) {
        Random random = new Random();
        return (Math.abs(random.nextInt()) % frequency);
    }

    public static String getRandomText(int length) {

        String base = "abcdefghijklmnopqrstuvwxyz0123456789";
        Random random = new Random();
        StringBuffer st = new StringBuffer("");
        for (int i = 0; i < length; i++) {
            int number = random.nextInt(base.length());
            st = st.append(base.charAt(number));
        }
        return st.toString();

    }
}