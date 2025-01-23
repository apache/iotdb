package org.apache.iotdb.db.query.eBUG;

import io.jsonwebtoken.lang.Assert;

import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.eBUG.buildEffectiveArea;

public class Test2 {
  // 用于测试在线采样
  public static void main(String[] args) {
    int n = 20;
    int eParam = 1;

    int m = 2; // >2代表在线采样，<=2代表预计算
    //        for (int m = 9900; m <= n; m++) {

    Polyline polyline = new Polyline();
    Random rand = new Random(2);
    for (int i = 0; i < n; i++) {
      double v = rand.nextInt(1000000);
      polyline.addVertex(new Point(i, v));
    }

    long startTime = System.currentTimeMillis();
    List<Point> results = buildEffectiveArea(polyline, eParam, false, m);
    long endTime = System.currentTimeMillis();
    System.out.println(
        "n="
            + n
            + ", e="
            + eParam
            + ", Time taken to reduce points: "
            + (endTime - startTime)
            + "ms");

    System.out.println(results.size());
    if (m > 2 && m < n) {
      Assert.isTrue(results.size() == m);
    } else {
      Assert.isTrue(results.size() == n);
    }
    if (results.size() <= 100) {
      System.out.println("+++++++++++++++++++");
      for (Point point : results) {
        System.out.println("Point: (" + point.x + ", " + point.y + ", " + point.z + ")");
      }
      // 注意现在返回的结果是按照Sig递增也就是bottom-up淘汰的顺序排列的，而不是按照时间戳递增排列
      // 按照时间戳递增排序整理:
      System.out.println("make the bottom-up results sorted by time:");
      results.sort(Comparator.comparingDouble(point -> point.x));
      System.out.println("+++++++++++++++++++");
      for (Point point : results) {
        System.out.println("Point: (" + point.x + ", " + point.y + ", " + point.z + ")");
      }
    }
  }
}
