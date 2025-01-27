package org.apache.iotdb.db.query.eBUG;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.db.query.eBUG.Tool.generateOutputFileName;

public class sample_MinMax {
  public static void main(String[] args) throws IOException {
    if (args.length < 7) {
      System.out.println(
          "Usage: Please provide arguments: inputFilePath,hasHeader,timeIdx,valueIdx,N,m,bucketType,(outDir)");
    }
    String input = args[0];
    boolean hasHeader = Boolean.parseBoolean(args[1]);
    int timeIdx = Integer.parseInt(args[2]);
    int valueIdx = Integer.parseInt(args[3]);
    int N = Integer.parseInt(args[4]); // N<=0表示读全部行，N>0表示读最多N行
    int m = Integer.parseInt(args[5]);
    String bucketTypeStr = args[6];
    MinMax.fixedBUCKETtype bucketType;
    if (bucketTypeStr.equals("width")) {
      bucketType = MinMax.fixedBUCKETtype.width;
    } else if (bucketTypeStr.equals("frequency")) {
      bucketType = MinMax.fixedBUCKETtype.frequency;
    } else {
      throw new IOException("please input fixed bucket type as width/frequency");
    }
    String outDir;
    if (args.length > 7) {
      outDir = args[7]; // 表示输出文件保存到指定的文件夹
    } else {
      outDir = null; // 表示输出文件保存到input文件所在的文件夹
    }

    // 打印信息供用户确认
    System.out.println("Input file: " + input);
    System.out.println("Has header: " + hasHeader);
    System.out.println("Time index: " + timeIdx);
    System.out.println("Value index: " + valueIdx);
    System.out.println("N: " + N);
    System.out.println("m: " + m);
    System.out.println("bucketType: " + bucketType);
    System.out.println("outDir: " + outDir);

    // 读取原始序列
    List<Point> points = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);

    String outputFile =
        generateOutputFileName(input, outDir, "-" + bucketType + "-n" + points.size() + "-m" + m);
    System.out.println("Output file: " + outputFile); // do not modify this hint string log

    // 采样
    List<Point> results;
    long startTime = System.currentTimeMillis();
    if (bucketType == MinMax.fixedBUCKETtype.width) {
      results = MinMax.reducePoints_equalWidthBucket(points, m, false);
    } else {
      results = MinMax.reducePoints_equalFrequencyBucket(points, m, false);
    }
    long endTime = System.currentTimeMillis();

    System.out.println("result point number: " + results.size());
    System.out.println(
        "n="
            + points.size()
            + ", m="
            + m
            + ", Time taken to reduce points: " // do not modify this hint string log
            + (endTime - startTime)
            + "ms");

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
      writer.write("x,y");
      writer.newLine();
      // 写入数据行，按顺序 x,y
      for (Point point : results) {
        writer.write(point.x + "," + point.y);
        writer.newLine();
      }
      System.out.println("CSV file written successfully to " + outputFile);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
