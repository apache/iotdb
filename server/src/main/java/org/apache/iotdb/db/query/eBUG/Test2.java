package org.apache.iotdb.db.query.eBUG;

import io.jsonwebtoken.lang.Assert;

import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.eBUG.buildEffectiveArea;

public class Test2 {
    // 用于测试在线采样
    public static void main(String[] args) {
        int n = 100000;
        int eParam = 1;

//        int m = 100; // >2代表在线采样，<=2代表预计算
        for (int m = 9900; m <= n; m++) {

            Polyline polyline = new Polyline();
            Random rand = new Random(2);
            for (int i = 0; i < n; i++) {
                double v = rand.nextInt(1000000);
                polyline.addVertex(new Point(i, v));
            }

            long startTime = System.currentTimeMillis();
            List<Point> results = buildEffectiveArea(polyline, eParam, false, m);
            long endTime = System.currentTimeMillis();
            System.out.println("n=" + n + ", e=" + eParam + ", Time taken to reduce points: " + (endTime - startTime) + "ms");

            System.out.println(results.size());
            Assert.isTrue(results.size() == m);
        }
//        if (results.size() <= 100) {
//            System.out.println("+++++++++++++++++++");
//            for (Point point : results) {
//                System.out.println("Point: (" + point.x + ", " + point.y + ", " + point.z + ")");
//            }
//        }
    }
}
