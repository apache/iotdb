package org.apache.iotdb.db.query.eBUG;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Stack;

public class Rdp {
  // Ramer–Douglas–Peucker algorithm (RDP)
  // top-down, for min perpendicular distance
  // worst case complexity n2
  // best case complexity nlogn

  public static double point_line_distance(Point point, Point lineStart, Point lineEnd) {
    // 计算点到直线的垂直正交距离
    // (x0,y0)到直线（经过(x1,y1)、(x2,y2)点的直线）的正交距离
    double x0 = point.x;
    double y0 = point.y;
    double x1 = lineStart.x;
    double y1 = lineStart.y;
    double x2 = lineEnd.x;
    double y2 = lineEnd.y;

    double numerator = Math.abs((x2 - x1) * (y1 - y0) - (x1 - x0) * (y2 - y1));
    double denominator = Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
    return numerator / denominator;
  }

  /**
   * 使用迭代实现 RDP 算法。避免递归深度过大。
   *
   * @param points 点集
   * @param epsilon 简化阈值
   * @return 简化后的点集
   */
  public static List<Point> reducePoints(List<Point> points, double epsilon, Object... kwargs) {
    if (points.size() <= 2) {
      return points;
    }

    // 使用栈模拟递归
    Stack<int[]> stack = new Stack<>();
    stack.push(new int[] {0, points.size() - 1});

    // 用于标记需要保留的点
    boolean[] keep = new boolean[points.size()];
    keep[0] = true;
    keep[points.size() - 1] = true;

    int count = 0;

    while (!stack.isEmpty()) {
      int[] range = stack.pop();
      int start = range[0];
      int end = range[1];

      // 找到距离最远的点
      double maxDistance = 0;
      int index = start;
      Point startPoint = points.get(start);
      Point endPoint = points.get(end);

      for (int i = start + 1; i < end; i++) {
        count++;
        double distance = point_line_distance(points.get(i), startPoint, endPoint);
        if (distance > maxDistance) {
          maxDistance = distance;
          index = i;
        }
      }

      // 如果最远距离大于阈值，则继续分割
      if (maxDistance > epsilon) {
        keep[index] = true; // 保留当前点
        stack.push(new int[] {start, index});
        stack.push(new int[] {index, end});
      }
    }

    // 构建简化后的点集
    List<Point> result = new ArrayList<>();
    for (int i = 0; i < points.size(); i++) {
      if (keep[i]) {
        result.add(points.get(i));
      }
    }

    System.out.println("count=" + count);
    return result;
  }

  public static Object[] normalizePoints(List<Point> points) {
    // 提取 x 和 y 值
    double[] xValues = points.stream().mapToDouble(p -> p.x).toArray();
    double[] yValues = points.stream().mapToDouble(p -> p.y).toArray();

    // 计算 x 和 y 的最小值和最大值
    double xMin = Double.MAX_VALUE, xMax = Double.MIN_VALUE;
    double yMin = Double.MAX_VALUE, yMax = Double.MIN_VALUE;

    for (double x : xValues) {
      if (x < xMin) xMin = x;
      if (x > xMax) xMax = x;
    }
    for (double y : yValues) {
      if (y < yMin) yMin = y;
      if (y > yMax) yMax = y;
    }

    // 标准化
    List<Point> normalizedPoints = new ArrayList<>();
    for (Point p : points) {
      double xNormalized = (xMax != xMin) ? (p.x - xMin) / (xMax - xMin) : 0;
      double yNormalized = (yMax != yMin) ? (p.y - yMin) / (yMax - yMin) : 0;
      normalizedPoints.add(new Point(xNormalized, yNormalized));
    }

    return new Object[] {normalizedPoints, xMin, xMax, yMin, yMax};
  }

  public static List<Point> denormalizePoints(List<Point> points, double[] originalScale) {
    double xMin = originalScale[0];
    double xMax = originalScale[1];
    double yMin = originalScale[2];
    double yMax = originalScale[3];

    List<Point> denormalizedPoints = new ArrayList<>();
    for (Point p : points) {
      double xOriginal = p.x * (xMax - xMin) + xMin;
      double yOriginal = p.y * (yMax - yMin) + yMin;
      denormalizedPoints.add(new Point(xOriginal, yOriginal));
    }

    return denormalizedPoints;
  }

  public static void main(String[] args) throws IOException {
    Random rand = new Random(10);
    String input = "D:\\datasets\\regular\\tmp2.csv";
    boolean hasHeader = true;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = 10;
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

    // 标准化
    Object[] normalizedData = normalizePoints(points);
    List<Point> normalizedPoints = (List<Point>) normalizedData[0];
    double xMin = (double) normalizedData[1];
    double xMax = (double) normalizedData[2];
    double yMin = (double) normalizedData[3];
    double yMax = (double) normalizedData[4];

    int m = 7;
    double tolerantPts = 0;
    double epsilon = Tool.getParam(normalizedPoints, m, Rdp::reducePoints, tolerantPts);
    //                double epsilon = 0.001;
    //        double epsilon = 0.004547272052729534;
    System.out.println("epsilon=" + epsilon);

    long startTime = System.currentTimeMillis();
    List<Point> sampled_tmp = reducePoints(normalizedPoints, epsilon);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime - startTime) + "ms");

    // 还原到原始尺度
    double[] originalScale = {xMin, xMax, yMin, yMax};
    List<Point> sampled = denormalizePoints(sampled_tmp, originalScale);

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

  //    public static void reducePoints(List<Point> points, double epsilon, int s, int e,
  // List<Point> resultList) {
  //        double dmax = 0;
  //        int index = 0;
  //
  //        final int start = s;
  //        final int end = e - 1;
  //        for (int i = start + 1; i < end; i++) {
  //            // Point
  //            final double px = list.get(i)[0];
  //            final double py = list.get(i)[1];
  //            // Start
  //            final double vx = list.get(start)[0];
  //            final double vy = list.get(start)[1];
  //            // End
  //            final double wx = list.get(end)[0];
  //            final double wy = list.get(end)[1];
  //            final double d = perpendicularDistance(px, py, vx, vy, wx, wy);
  //            if (d > dmax) {
  //                index = i;
  //                dmax = d;
  //            }
  //        }
  //        // If max distance is greater than epsilon, recursively simplify
  //        if (dmax > epsilon) {
  //            // Recursive call
  //            douglasPeucker(list, s, index, epsilon, resultList);
  //            douglasPeucker(list, index, e, epsilon, resultList);
  //        } else {
  //            if ((end - start) > 0) {
  //                resultList.add(list.get(start));
  //                resultList.add(list.get(end));
  //            } else {
  //                resultList.add(list.get(start));
  //            }
  //        }
  //    }

  //    /**
  //     * Given a curve composed of line segments find a similar curve with fewer points.
  //     *
  //     * @param list    List of Double[] points (x,y)
  //     * @param epsilon Distance dimension
  //     * @return Similar curve with fewer points
  //     */
  //    public static final List<Double[]> douglasPeucker(List<Double[]> list, double epsilon) {
  //        final List<Double[]> resultList = new ArrayList<Double[]>();
  //        douglasPeucker(list, 0, list.size(), epsilon, resultList);
  //        return resultList;
  //    }

}
