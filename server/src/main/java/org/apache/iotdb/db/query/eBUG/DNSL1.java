package org.apache.iotdb.db.query.eBUG;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.DP.dynamic_programming;

public class DNSL1 {
  // 默认使用linear interpolation连接分段首尾点来近似
  // 默认使用L1 error
  // 使用divide and conquer算法
  public static List<Point> reducePoints(List<Point> points, int m, boolean debug) {
    int n = points.size();
    int k = m - 1; // 分段数
    int intervalNum = (int) Math.pow((double) n / k, (double) 2 / 3); // divide batch点数
    int intervalPts = n / intervalNum;

    if (debug) {
      System.out.println("interval point length=" + intervalPts);
    }

    // divide into intervalPts equal-length intervals
    List<Point> allSampledPoints = new ArrayList<>();
    for (int start = 0; start < n; start += intervalPts) {
      int end = Math.min(start + intervalPts, n);
      List<Point> batch = points.subList(start, end); // 左闭右开
      if (debug) {
        System.out.println("Processing batch: [" + start + "," + end + ")");
      }
      List<Point> sampledForBatch = dynamic_programming(batch, k, DP.ERRORtype.L1, false);
      allSampledPoints.addAll(sampledForBatch); // 把上一步的结果加到大的容器里
    }

    // 在大的容器上最后执行DP.dynamic_programming得到最后的m个采样点
    if (debug) {
      System.out.println("number of points from batch sampling: " + allSampledPoints.size());
    }

    // TODO 但是注意这里用的是linear interpolation近似，而不是DNS原文的constant
    List<Point> finalSampledPoints =
        dynamic_programming(allSampledPoints, k, DP.ERRORtype.L1, false);
    if (debug) {
      System.out.println("result point number: " + finalSampledPoints.size());
    }
    return finalSampledPoints;
  }

  public static void main(String[] args) {
    Random rand = new Random(10);
    String input = "raw.csv";
    boolean hasHeader = true;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = 100_0000;
    //        List<Point> points = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
    Polyline polyline = new Polyline();
    for (int i = 0; i < N; i += 1) {
      double v = rand.nextInt(1000);
      polyline.addVertex(new Point(i, v));
    }
    List<Point> points = polyline.getVertices();
    try (FileWriter writer = new FileWriter("raw.csv")) {
      // 写入CSV头部
      writer.append("x,y,z\n");

      // 写入每个点的数据
      for (Point point : points) {
        writer.append(point.x + "," + point.y + "," + point.z + "\n");
      }
      System.out.println(points.size() + " Data has been written");
    } catch (IOException e) {
      System.out.println("Error writing to CSV file: " + e.getMessage());
    }

    //        int m = (int) Math.pow(points.size(), 0.5);
    int m = 1_000;

    //        long startTime = System.currentTimeMillis();
    //        List<Point> sampled = dynamic_programming(points, m - 1, DP.ERROR.area, false);
    //        long endTime = System.currentTimeMillis();
    //        System.out.println("Time taken: " + (endTime - startTime) + "ms");
    //        for (Point p : sampled) {
    //            System.out.println(p);
    //        }
    //        System.out.println(sampled.size());

    long startTime2 = System.currentTimeMillis();
    //        System.out.println(points.size() * m / (Math.pow(points.size() * 1.0 / (m - 1), 2 /
    // 3.0)));
    //        System.out.println(10000000 * (2 / (Math.pow(10000000 * 1.0 / (2 - 1), 2 / 3.0))));
    List<Point> sampled2 = reducePoints(points, m, true);
    long endTime2 = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime2 - startTime2) + "ms");
    //        for (Point p : sampled2) {
    //            System.out.println(p);
    //        }
    //        System.out.println(sampled2.size());
  }
}
