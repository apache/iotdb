package org.apache.iotdb.db.query.eBUG;

import java.io.*;
import java.util.List;
import java.util.Random;

import static org.apache.iotdb.db.query.eBUG.Tool.getParam;

public class Test9 {
  // 测试swab二分查找maxError参数效果
  public static void main(String[] args) throws IOException {
    Random rand = new Random(10);
    String input = "D:\\datasets\\regular\\tmp2.csv";
    boolean hasHeader = false;
    int timeIdx = 0;
    int valueIdx = 1;
    int N = 1000_0000;
    //        Polyline polyline = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
    Polyline polyline = new Polyline();
    for (int i = 0; i < N; i += 1) {
      double v = rand.nextInt(1000);
      polyline.addVertex(new Point(i, v));
    }
    try (FileWriter writer = new FileWriter("raw.csv")) {
      // 写入CSV头部
      writer.append("x,y,z\n");

      // 写入每个点的数据
      for (int i = 0; i < polyline.size(); i++) {
        Point point = polyline.get(i);
        writer.append(point.x + "," + point.y + "," + point.z + "\n");
      }
      System.out.println(polyline.size() + " Data has been written");
    } catch (IOException e) {
      System.out.println("Error writing to CSV file: " + e.getMessage());
    }

    // epsilon不能太小，否则每次input进来很少的点，循环太多次
    // 另外buffer不能太大，也就是n/m不能太大，否则每次bottomUp在一个很大的buffer上
    int m = 1000;
    //        double epsilon = 1000000000; // 这个大了没关系，buffer size会控制住进来的点数的

    long startTime = System.currentTimeMillis();
    double epsilon = getParam(polyline.getVertices(), m, SWAB::reducePoints, m * 0.1, m);
    long endTime = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime - startTime) + "ms");

    startTime = System.currentTimeMillis();
    List<Point> sampled = SWAB.reducePoints(polyline.getVertices(), epsilon, m);
    endTime = System.currentTimeMillis();
    System.out.println("Time taken: " + (endTime - startTime) + "ms");
    System.out.println(sampled.size());
    System.out.println(epsilon);

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
