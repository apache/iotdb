package cn.edu.tsinghua.iotdb.utils;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by zhangjinrui on 2018/1/25.
 */
public class PrimitiveArrayListTest {

    @Test
    public void test1() {

        long timestamp = System.currentTimeMillis();
        int count = 10000;
        PrimitiveArrayList primitiveArrayList = new PrimitiveArrayList(int.class);
        for (int i = 0; i < count; i++) {
            primitiveArrayList.putTimestamp(i, i);
        }
//        System.out.println("Time used: " + (System.currentTimeMillis() - timestamp) + "ms");

        for (int i = 0; i < count; i++) {
            int v = (int) primitiveArrayList.getValue(i);
            Assert.assertEquals((long) i, primitiveArrayList.getTimestamp(i));
            Assert.assertEquals(i, v);
        }
//        System.out.println("Time used: " + (System.currentTimeMillis() - timestamp) + "ms");
        printMemused();
    }

    public static void printMemused() {
        Runtime.getRuntime().gc();
        long size = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        int gb = (int) (size / 1024 / 1024 / 1024);
        int mb = (int) (size / 1024 / 1024 - gb * 1024);
        int kb = (int) (size / 1024 - gb * 1024 * 1024 - mb * 1024);
        int b = (int) (size - gb * 1024 * 1024 * 1024 - mb * 1024 * 1024 - kb * 1024);
        System.out.println("Mem Used:" + gb + "GB, " + mb + "MB, " + kb + "KB, " + b + "B");
    }
}
