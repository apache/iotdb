package org.apache.iotdb.db.query.eBUG;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;

public class Tmp {
  // 用于debug为什么n>2kw的耗时增大明显
  public static void main(String[] args) {
    int seed = 10;
    Random rand = new Random(seed);
    int eParam = 1;
    try (PrintWriter writer = new PrintWriter(new File("tmp.csv"))) {
      for (int n = 100_0000;
          n < 100000_0000;
          n += 5000_0000) { // 超过两千万就都变得慢多了，感觉可能是内存有限的原因，而不是算法复杂度
        //            for (int n = 19000000; n < 3000_0000; n +=200_0000) {
        PriorityQueue<Point> heap = new PriorityQueue<>(Comparator.comparingDouble(t -> t.y));
        for (int i = 0; i < n; i++) {
          double v = rand.nextInt(1000000);
          heap.add(new Point(i, v));
        }

        long startTime = System.currentTimeMillis();
        long sum = 0;
        while (!heap.isEmpty()) {
          Point p = heap.poll();
          sum += p.x;
        }
        long endTime = System.currentTimeMillis();
        System.out.println(n + ", Time taken to reduce points: " + (endTime - startTime) + "ms");
        writer.println(n + "," + "0" + "," + (endTime - startTime));
        System.out.println(sum);
      }
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }

    System.out.println("finish");
  }
}
