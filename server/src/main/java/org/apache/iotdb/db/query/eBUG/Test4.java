package org.apache.iotdb.db.query.eBUG;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.SWAB.myLA_withTimestamps;
import static org.apache.iotdb.db.query.eBUG.SWAB.prefixSum;

public class Test4 {
  // 用于测试使用prefix sum加速L2误差计算的加速效果
  public static void main(String[] args) {
    Random rand = new Random(10);
    String input = "D:\\datasets\\regular\\tmp2.csv";
    boolean hasHeader = false;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = 2000_0000;
    //        Polyline polyline = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
    Polyline polyline = new Polyline();
    for (int i = 0; i < N; i += 1) {
      double v = rand.nextInt(1000);
      polyline.addVertex(new Point(i, v));
    }
    //        try (FileWriter writer = new FileWriter("raw.csv")) {
    //            // 写入CSV头部
    //            writer.append("x,y,z\n");
    //
    //            // 写入每个点的数据
    //            for (int i = 0; i < polyline.size(); i++) {
    //                Point point = polyline.get(i);
    //                writer.append(point.x + "," + point.y + "," + point.z + "\n");
    //            }
    //            System.out.println(polyline.size() + " Data has been written");
    //        } catch (IOException e) {
    //            System.out.println("Error writing to CSV file: " + e.getMessage());
    //        }

    long startTime = System.currentTimeMillis();
    Object[] prefixSum = prefixSum(polyline.getVertices());
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken to precompute prefix sum: " + (endTime - startTime) + "ms");

    double error;

    List<Double> error1 = new ArrayList<>();
    startTime = System.currentTimeMillis();
    for (int lx = 0; lx < polyline.size() / 2; lx += 10000) {
      error = myLA_withTimestamps(polyline.getVertices(), prefixSum, lx, polyline.size() - 1);
      System.out.println(lx + "," + error);
      error1.add(error);
    }
    endTime = System.currentTimeMillis();
    System.out.println(
        "Time taken to compute L2 error with prefix sum: " + (endTime - startTime) + "ms");

    List<Double> error2 = new ArrayList<>();
    startTime = System.currentTimeMillis();
    for (int lx = 0; lx < polyline.size() / 2; lx += 10000) {
      error = myLA_withTimestamps(polyline.getVertices(), lx, polyline.size() - 1);
      System.out.println(lx + "," + error);
      error2.add(error);
    }
    endTime = System.currentTimeMillis();
    System.out.println(
        "Time taken to compute L2 error without prefix sum: " + (endTime - startTime) + "ms");

    try (PrintWriter writer = new PrintWriter(new File("output.csv"))) {
      // 写入字符串
      for (int i = 0; i < error1.size(); i++) {
        writer.println(error1.get(i) + "," + error2.get(i) + "," + (error2.get(i) - error1.get(i)));
      }

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    }
  }
}
