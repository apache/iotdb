package org.apache.iotdb.db.query.eBUG;

import java.io.*;
import java.util.*;

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

  public static List<Integer> getLtdBinIdxs(List<Point> points, int m, int maxIter, boolean debug)
      throws IOException {
    if (m < 6) {
      throw new IOException("m at least 6");
    }
    int n = points.size();
    int numOfIterations;
    if (maxIter >= 0) {
      numOfIterations = maxIter; // overwrite
    } else {
      numOfIterations = (int) (n * 1.0 / m * 10);
    }
    if (debug) {
      System.out.println("numOfIterations=" + numOfIterations);
    }

    int nbins = m - 2; // 留出全局首尾点

    double blockSize = (n - 3) * 1.0 / nbins;
    List<Integer> offset = new LinkedList<>(); // nbins个分桶，nbins+1个桶边界
    for (double i = 1; i < n; i += blockSize) {
      offset.add((int) i); // 1~n-2, 这样最后一个offset+1才不会超出边界
    }
    if (debug) {
      System.out.println("init bins: " + offset);
    }

    LTDBucket[] buckets = new LTDBucket[nbins];
    double lastSSE = -1;
    for (int i = 0; i < nbins - 1; i++) {
      double sse;
      if (i == 0) {
        sse = calculateSSEForBucket(points, offset.get(i) - 1, offset.get(i + 1) + 1);
      } else {
        sse = lastSSE;
      }
      double sse_next = calculateSSEForBucket(points, offset.get(i + 1) - 1, offset.get(i + 2) + 1);
      double sum = sse + sse_next;
      buckets[i] = new LTDBucket(offset.get(i), offset.get(i + 1), sse, sum);
      lastSSE = sse_next;
    }
    // the last bucket sumOf2SSE set as infinity meaning never merged
    buckets[nbins - 1] =
        new LTDBucket(offset.get(nbins - 1), offset.get(nbins), lastSSE, Double.MAX_VALUE);

    // 设置前后关系
    LTDBucket starter = new LTDBucket(0, 0, 0, 0);
    starter.next = buckets[0];
    for (int i = 0; i < nbins; i++) {
      buckets[i].prev = (i == 0 ? starter : buckets[i - 1]);
      buckets[i].next = (i == nbins - 1 ? null : buckets[i + 1]);
    }

    //        System.out.println("begin creating heap...");

    // 使用优先队列构建
    PriorityQueue<LTDBucket> splitHeap =
        new PriorityQueue<>((p1, p2) -> Double.compare(p2.sse, p1.sse));
    // 越大的排在前面
    Collections.addAll(splitHeap, buckets);
    PriorityQueue<LTDBucket> mergeHeap =
        new PriorityQueue<>(Comparator.comparingDouble(p -> p.sumOf2SSE));
    // 越小的排在前面
    Collections.addAll(mergeHeap, buckets);

    //        System.out.println("begin iterating...");

    for (int c = 0; c < numOfIterations; c++) {
      if (debug) {
        System.out.println("--------------[" + c + "]----------------");
      }
      if (splitHeap.isEmpty() || mergeHeap.isEmpty()) {
        break;
      }
      // Find the bucket to be split and buckets to be merged
      LTDBucket bucket_split = splitHeap.poll();
      if (bucket_split.isDeleted) {
        c--;
        continue;
      }
      LTDBucket buckets_merged = mergeHeap.poll();
      if (buckets_merged.isDeleted) {
        c--;
        splitHeap.add(bucket_split); // 前面poll出来的未被删除的要加回去！
        continue;
      }
      // TODO
      if (bucket_split.startIdx == buckets_merged.startIdx) {
        if (buckets_merged.next.next != null) {
          buckets_merged = buckets_merged.next;
        } else {
          buckets_merged = buckets_merged.prev;
        }
      }
      if (bucket_split.endIdx == buckets_merged.getMergedEndIdx()) {
        if (buckets_merged.prev != starter) {
          buckets_merged = buckets_merged.prev;
        } else { // 假设nbins至少>=4, 于是buckets_merged.next.next不为null
          buckets_merged = buckets_merged.next.next;
        }
      }

      // split
      if (debug) {
        System.out.println("+++To split bucket: " + bucket_split);
      }
      int startIdx = bucket_split.startIdx;
      int endIdx = bucket_split.endIdx;
      int middleIdx = (int) ((startIdx + endIdx) * 1.0 / 2);
      double sse1 = calculateSSEForBucket(points, startIdx - 1, middleIdx + 1);
      double sse2 = calculateSSEForBucket(points, middleIdx - 1, endIdx + 1);

      LTDBucket newBucket =
          new LTDBucket(middleIdx, endIdx, sse2, sse2 + bucket_split.getNextSSE());
      newBucket.prev = bucket_split;
      newBucket.next = bucket_split.next;
      if (newBucket.next != null) {
        newBucket.next.prev = newBucket;
      }
      bucket_split.next = newBucket;

      bucket_split.isDeleted = true;
      LTDBucket replaceBucket = new LTDBucket(bucket_split);
      replaceBucket.endIdx = middleIdx;
      replaceBucket.sse = sse1;
      replaceBucket.sumOf2SSE = sse1 + sse2;
      replaceBucket.next = newBucket;
      // 更新前一个桶的sumOf2SSE 注意这意味着前一个桶也要更新heap！
      if (replaceBucket.prev != starter) {
        replaceBucket.prev.isDeleted = true;
        LTDBucket preReplaceBucket = new LTDBucket(replaceBucket.prev);
        preReplaceBucket.sumOf2SSE = preReplaceBucket.sse + sse1;
        splitHeap.add(preReplaceBucket);
        mergeHeap.add(preReplaceBucket);
      }

      splitHeap.add(newBucket);
      splitHeap.add(replaceBucket);
      mergeHeap.add(newBucket);
      mergeHeap.add(replaceBucket);

      if (debug) {
        System.out.println("\tsplit into: " + replaceBucket + "," + newBucket);
      }

      // merge
      if (debug) {
        System.out.println("---To merge bucket: " + buckets_merged + "," + buckets_merged.next);
      }
      startIdx = buckets_merged.startIdx;
      endIdx = buckets_merged.getMergedEndIdx();
      double sse3 = calculateSSEForBucket(points, startIdx - 1, endIdx + 1);
      buckets_merged.isDeleted = true;
      buckets_merged.next.isDeleted = true;
      LTDBucket mergedBucket = new LTDBucket(buckets_merged);
      mergedBucket.endIdx = endIdx;
      mergedBucket.sse = sse3;
      mergedBucket.next = buckets_merged.next.next;
      if (mergedBucket.next != null) {
        mergedBucket.next.prev = mergedBucket;
      }
      // 更新自己的sumOf2SSE
      mergedBucket.sumOf2SSE = sse3 + mergedBucket.getNextSSE(); // 如果next为null，这一项就是Infinity
      // 更新前一个分桶的sumOf2SSE 注意这意味着前一个桶也要更新heap！
      if (mergedBucket.prev != starter) {
        mergedBucket.prev.isDeleted = true;
        LTDBucket preReplaceBucket = new LTDBucket(mergedBucket.prev);
        preReplaceBucket.sumOf2SSE = preReplaceBucket.sse + sse3;
        splitHeap.add(preReplaceBucket);
        mergeHeap.add(preReplaceBucket);
      }
      splitHeap.add(mergedBucket);
      mergeHeap.add(mergedBucket);
      if (debug) {
        System.out.println("\tmerged bucket: " + mergedBucket);
      }
      //            System.out.println("----------" + splitHeap.size() + "-------------");
      //            System.out.println("----------" + mergeHeap.size() + "-------------");
    }

    List<Integer> res = new ArrayList<>();
    LTDBucket bucket = starter.next;
    while (true) {
      res.add(bucket.startIdx);
      if (bucket.next == null) {
        res.add(bucket.endIdx); // 收尾
        break;
      }
      bucket = bucket.next;
    }
    if (debug) {
      System.out.println(res);
    }
    return res;
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

  public static List<Point> LTD(List<Point> points, int m, int maxIter, boolean debug)
      throws IOException {
    List<Integer> bins = getLtdBinIdxs(points, m, maxIter, debug);
    return LTTB(points, bins);
  }

  public static void main(String[] args) throws IOException {
    Random rand = new Random(10);
    String input = "D:\\datasets\\regular\\tmp2.csv";
    boolean hasHeader = true;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = 1000_0;
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

    int m = 1000;
    int maxIter = -1;
    long startTime = System.currentTimeMillis();
    //        List<Integer> bins = getLtdBinIdxs(points, m, true);
    List<Point> sampled = LTD(points, m, maxIter, false);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime - startTime) + "ms");

    //    for (Point p : sampled) {
    //      System.out.println(p);
    //    }

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
