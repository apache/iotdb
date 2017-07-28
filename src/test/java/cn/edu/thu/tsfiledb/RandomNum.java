package cn.edu.thu.tsfiledb;

import java.util.Random;

public class RandomNum {

    Random random = new Random();

    public long getRandomLong(long min, long max) {
        return Math.abs(random.nextLong()) % (max-min+1) + min;
    }

    public int getRandomInt(int min, int max) {
        return Math.abs(random.nextInt() % (max-min+1) + min);
    }

    public double getRandomDouble(double min, double max) {
        return (min + Math.random() * (max - min));
    }

    public static void main(String[] argc) {

        RandomNum r = new RandomNum();

        for (int i = 0; i < 10; i++) {
            //System.out.println(r.getRandomLong(1,100));
            System.out.println(r.getRandomDouble(1,5));
        }
        System.out.println(System.currentTimeMillis());
    }

}