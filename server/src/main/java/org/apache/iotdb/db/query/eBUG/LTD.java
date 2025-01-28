package org.apache.iotdb.db.query.eBUG;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class LTD {
  public static Point calculateAveragePoint(List<Point> points, int startClosed, int endClosed) {
    double sumX = 0;
    double sumY = 0;
    for (int i = startClosed; i <= endClosed; i++) {
      sumX += points.get(i).x;
      sumY += points.get(i).y;
    }
    int len = endClosed - startClosed + 1;
    return new Point(sumX / len, sumY / len);
  }

  public static double[] calculateLinearRegressionCoefficients(
      List<Point> points, int startClosed, int endClosed) {
    Point averages = calculateAveragePoint(points, startClosed, endClosed);
    double avgX = averages.x;
    double avgY = averages.y;

    double aNumerator = 0.0;
    double aDenominator = 0.0;
    for (int i = startClosed; i <= endClosed; i++) {
      aNumerator += (points.get(i).x - avgX) * (points.get(i).y - avgY);
      aDenominator += (points.get(i).x - avgX) * (points.get(i).x - avgX);
    }

    double a = aNumerator / aDenominator;
    double b = avgY - a * avgX;

    return new double[] {a, b};
  }

  public static double calculateSSEForBucket(List<Point> points, int startClosed, int endClosed) {
    double[] coefficients = calculateLinearRegressionCoefficients(points, startClosed, endClosed);
    double a = coefficients[0];
    double b = coefficients[1];

    double sse = 0.0;

    for (int i = startClosed; i <= endClosed; i++) {
      double error = points.get(i).y - (a * points.get(i).x + b);
      sse += error * error;
    }

    return sse;
  }

  public static List<Integer> getLtdBinIdxs(List<Point> points, int m, int maxIter, boolean debug) {
    int numOfIterations = (int) (points.size() * 1.0 / m * 10);
    if (maxIter >= 0) {
      //      numOfIterations = Math.min(numOfIterations, maxIter);
      numOfIterations = maxIter; // overwrite
    }
    double blockSize = (points.size() - 3) * 1.0 / (m - 2);

    List<Integer> offset = new LinkedList<>();
    for (double i = 1; i < points.size(); i += blockSize) {
      offset.add((int) i); // 1~n-2, 这样最后一个offset+1才不会超出边界
    }
    if (debug) {
      System.out.println("numOfIterations=" + numOfIterations);
      System.out.println(offset);
    }

    List<Double> sse = new LinkedList<>();

    // Initialization
    for (int i = 0; i < m - 2; i++) {
      // with one extra point overlapping for each adjacent bucket
      sse.add(calculateSSEForBucket(points, offset.get(i) - 1, offset.get(i + 1) + 1));
    }

    for (int c = 0; c < numOfIterations; c++) {
      // Find the bucket to be split
      int maxSSEIndex = -1;
      double maxSSE = Double.NEGATIVE_INFINITY;
      for (int i = 0; i < m - 2; i++) {
        if (offset.get(i + 1) - offset.get(i) <= 1) {
          continue;
        }
        if (sse.get(i) > maxSSE) {
          maxSSE = sse.get(i);
          maxSSEIndex = i;
        }
      }
      if (maxSSEIndex < 0) {
        if (debug) {
          System.out.println(c);
          System.out.println(maxSSEIndex);
          System.out.println("break max");
        }
        break;
      }

      // Find the buckets to be merged
      int minSSEIndex = -1;
      double minSSE = Double.POSITIVE_INFINITY;
      for (int i = 0; i < m - 3; i++) {
        if (i == maxSSEIndex || i + 1 == maxSSEIndex) {
          continue;
        }
        if (sse.get(i) + sse.get(i + 1) < minSSE) {
          minSSE = sse.get(i) + sse.get(i + 1);
          minSSEIndex = i;
        }
      }
      if (minSSEIndex < 0) {
        if (debug) {
          System.out.println(c);
          System.out.println(minSSEIndex);
          System.out.println("break min");
        }
        break;
      }

      // Split
      int startIdx = offset.get(maxSSEIndex);
      int endIdx = offset.get(maxSSEIndex + 1);
      int middleIdx = (startIdx + endIdx) / 2;
      offset.add(maxSSEIndex + 1, middleIdx);

      // Update SSE affected by split
      sse.set(
          maxSSEIndex,
          calculateSSEForBucket(
              points, offset.get(maxSSEIndex) - 1, offset.get(maxSSEIndex + 1) + 1));

      double newSse =
          calculateSSEForBucket(
              points, offset.get(maxSSEIndex + 1) - 1, offset.get(maxSSEIndex + 2) + 1);

      sse.add(maxSSEIndex + 1, newSse);

      // Merge
      if (minSSEIndex > maxSSEIndex) {
        minSSEIndex += 1; // Adjust index
      }
      offset.remove(minSSEIndex + 1);

      sse.set(
          minSSEIndex,
          calculateSSEForBucket(
              points, offset.get(minSSEIndex) - 1, offset.get(minSSEIndex + 1) + 1));

      sse.remove(minSSEIndex + 1);
    }

    // Convert ArrayList to int[] and return
    return offset;
  }

  public static List<Point> LTTB(List<Point> points, List<Integer> bins) {
    List<Point> res = new ArrayList<>();
    res.add(points.get(0));

    for (int i = 0; i < bins.size() - 1; i++) {
      Point leftAnchor = res.get(res.size() - 1);
      Point rightFloater;
      if (i == bins.size() - 2) {
        rightFloater = points.get(points.size() - 1);
      } else {
        rightFloater = calculateAveragePoint(points, bins.get(i + 1), bins.get(i + 2));
      }
      int bestIdx = -1;
      double maxArea = -Double.MAX_VALUE;
      for (int j = bins.get(i); j < bins.get(i + 1); j++) {
        double area = Tool.triArea(points.get(j), leftAnchor, rightFloater);
        if (area > maxArea) {
          maxArea = area;
          bestIdx = j;
        }
      }
      if (bestIdx > 0) {
        res.add(points.get(bestIdx));
      }
    }
    res.add(points.get(points.size() - 1));
    return res;
  }

  public static List<Point> LTD(List<Point> points, int m, int maxIter, boolean debug) {
    List<Integer> bins = getLtdBinIdxs(points, m, maxIter, debug);
    return LTTB(points, bins);
  }

  public static void main(String[] args) {
    Random rand = new Random(10);
    String input = "D:\\datasets\\regular\\tmp2.csv";
    boolean hasHeader = true;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = 10000;
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
    int maxIter = 10;
    long startTime = System.currentTimeMillis();
    //        List<Integer> bins = getLtdBinIdxs(points, m, true);
    List<Point> sampled = LTD(points, m, maxIter, true);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime - startTime) + "ms");

    for (Point p : sampled) {
      System.out.println(p);
    }

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
