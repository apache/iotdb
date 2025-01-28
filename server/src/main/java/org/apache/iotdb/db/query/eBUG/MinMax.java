package org.apache.iotdb.db.query.eBUG;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MinMax {
  public enum fixedBUCKETtype {
    width,
    frequency,
  }

  public static List<Point> reducePoints_equalFrequencyBucket(
      List<Point> points, int m, boolean debug) {
    // 不算首尾点，还要(m-2)/2个分桶，每个分桶里采集MinMax两个点
    int numOfBuckets = (int) ((m - 2) / 2.0);

    // 等宽分桶
    int frequency = (int) Math.ceil(points.size() * 1.0 / numOfBuckets);

    if (debug) {
      System.out.println("numOfBuckets=" + numOfBuckets);
      System.out.println("frequency=" + frequency);
    }

    List<Point> res = new ArrayList<>();
    res.add(points.get(0));

    int currentBucketIdx = 0;
    double currentBucketStartIdx = currentBucketIdx * frequency;
    double currentBucketEndIdx = currentBucketStartIdx + frequency;
    double minValue = Double.MAX_VALUE;
    int minIdx = -1;
    double maxValue = -Double.MAX_VALUE; // Double.MIN_VALUE is positive so do not use it!!!
    int maxIdx = -1;
    for (int i = 0; i < points.size() - 1; i++) { // 注意到倒数第二个点为止，因为如果到全局尾点，由于精度出入会有bug
      Point p = points.get(i);
      while (i >= currentBucketEndIdx) {
        // record the results of the last bucket
        if (minIdx != -1) {
          if (minIdx < maxIdx) {
            res.add(points.get(minIdx));
            res.add(points.get(maxIdx));
          } else {
            res.add(points.get(maxIdx));
            res.add(points.get(minIdx));
          }
        }
        if (debug) {
          System.out.println(
              currentBucketStartIdx
                  + ","
                  + currentBucketEndIdx
                  + ","
                  + currentBucketIdx
                  + ","
                  + res.size());
        }
        // find the bucket that holds p, and reset for this new bucket
        currentBucketIdx++;
        currentBucketStartIdx = currentBucketIdx * frequency;
        currentBucketEndIdx = currentBucketStartIdx + frequency;
        minValue = Double.MAX_VALUE;
        maxValue = -Double.MAX_VALUE;
        minIdx = -1;
        maxIdx = -1;
      }
      if (p.y < minValue) {
        minValue = p.y;
        minIdx = i;
      }
      if (p.y > maxValue) {
        maxValue = p.y;
        maxIdx = i;
      }
    }

    // record the results of the last bucket
    if (minIdx < maxIdx) {
      res.add(points.get(minIdx));
      res.add(points.get(maxIdx));
    } else {
      res.add(points.get(maxIdx));
      res.add(points.get(minIdx));
    }
    if (debug) {
      System.out.println(
          currentBucketStartIdx
              + ","
              + currentBucketEndIdx
              + ","
              + currentBucketIdx
              + ","
              + res.size());
    }

    // record the last point
    res.add(points.get(points.size() - 1));

    return res;
  }

  public static List<Point> reducePoints_equalWidthBucket(
      List<Point> points, int m, boolean debug) {
    double startTime = points.get(0).x;
    double endTime = points.get(points.size() - 1).x;

    // 不算首尾点，还要(m-2)/2个分桶，每个分桶里采集MinMax两个点
    int numOfBuckets = (int) ((m - 2) / 2.0);

    // 等宽分桶
    double interval = (endTime - startTime) / numOfBuckets;

    if (debug) {
      System.out.println("numOfBuckets=" + numOfBuckets);
      System.out.println("width=" + interval);
    }

    List<Point> res = new ArrayList<>();
    res.add(points.get(0));

    int currentBucketIdx = 0;
    double currentBucketStartTime = currentBucketIdx * interval;
    double currentBucketEndTime = currentBucketStartTime + interval;
    double minValue = Double.MAX_VALUE;
    int minIdx = -1;
    double maxValue = -Double.MAX_VALUE; // Double.MIN_VALUE is positive so do not use it!!!
    int maxIdx = -1;
    for (int i = 0; i < points.size() - 1; i++) { // 注意到倒数第二个点为止，因为如果到全局尾点，由于精度出入会有bug
      Point p = points.get(i);
      while (p.x >= currentBucketEndTime) {
        // record the results of the last bucket
        if (minIdx != -1) {
          if (minIdx < maxIdx) {
            res.add(points.get(minIdx));
            res.add(points.get(maxIdx));
          } else {
            res.add(points.get(maxIdx));
            res.add(points.get(minIdx));
          }
        }
        if (debug) {
          System.out.println(
              currentBucketStartTime
                  + ","
                  + currentBucketEndTime
                  + ","
                  + currentBucketIdx
                  + ","
                  + res.size());
        }
        // find the bucket that holds p, and reset for this new bucket
        currentBucketIdx++;
        currentBucketStartTime = currentBucketIdx * interval;
        currentBucketEndTime = currentBucketStartTime + interval;
        minValue = Double.MAX_VALUE;
        maxValue = -Double.MAX_VALUE;
        minIdx = -1;
        maxIdx = -1;
      }
      if (p.y < minValue) {
        minValue = p.y;
        minIdx = i;
      }
      if (p.y > maxValue) {
        maxValue = p.y;
        maxIdx = i;
      }
    }

    // record the results of the last bucket
    if (minIdx < maxIdx) {
      res.add(points.get(minIdx));
      res.add(points.get(maxIdx));
    } else {
      res.add(points.get(maxIdx));
      res.add(points.get(minIdx));
    }
    if (debug) {
      System.out.println(
          currentBucketStartTime
              + ","
              + currentBucketEndTime
              + ","
              + currentBucketIdx
              + ","
              + res.size());
    }

    // record the last point
    res.add(points.get(points.size() - 1));

    return res;
  }

  public static void main(String[] args) {
    Random rand = new Random(10);
    String input = "D:\\datasets\\regular\\tmp2.csv";
    boolean hasHeader = true;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = -1;
    List<Point> points = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
    //        Polyline polyline = new Polyline();
    //        for (int i = 0; i < N; i += 1) {
    //            double v = rand.nextInt(1000);
    //            polyline.addVertex(new Point(i, v));
    //        }
    //        List<Point> points = polyline.getVertices();
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
    int m = 10;

    long startTime = System.currentTimeMillis();
    List<Point> sampled = reducePoints_equalFrequencyBucket(points, m, false);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime - startTime) + "ms");

    //        for (Point p : sampled) {
    //            System.out.println(p);
    //        }
    System.out.println(sampled.size());

    try (PrintWriter writer = new PrintWriter(new File("output.csv"))) {
      // 写入字符串
      for (int i = 0; i < sampled.size(); i++) {
        writer.println(sampled.get(i).x + "," + sampled.get(i).y);
      }

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
