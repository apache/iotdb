package cn.edu.thu.tsfiledb.engine.overflow;

import org.junit.Test;

/**
 * @author CGF.
 */
public class IntervalForceVerify {

    private int[] value = new int[1000];
    private int b1 = 1;
    private int b2 = 2;
    private int b3 = 3;
    private int b4 = 4;
    private int b5 = 5;

    private void delete(int time) {
        for (int i = 0;i < time;i ++) {
            value[i] = -1;
        }
    }

    private void update(int t1, int t2, int v) {
        for (int i = t1;i <= t2;i ++) {
            if (value[i] != -1)
                value[i] = v;
        }
    }

    private void insert(int t, int v) {
        value[t] = v;
    }

    @Test
    public void forceVerify() {
        insert(0, b1);
        update(5, 10, b1);
        update(15, 25, b1);
        update(30, 55, b1);
        update(70, 80, b1);
        update(83, 90, b1);
        insert(95, b1);
        insert(105, b1);
        insert(110, b1);

        update(100, 110, b2);

        delete(45);
        update(60, 70, b3);
        update(95, 110, b3);

        update(35, 50, b4);

        delete(20);
        insert(30, b5);
        insert(40, b5);
        update(60, 65, b5);
        update(75, 85, b5);
        insert(95, b5);

//        for (int i = 0;i < 110;i ++) {
//            if (value[i] != 0)
//                System.out.println(i + ":" + value[i]);
//        }
    }
}
