package org.apache.iotdb.db.query.eBUG;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.db.query.eBUG.Tool.generateOutputFileName;

public class sample_bottomUpYdiff {
  public static void main(String[] args) throws IOException {
    if (args.length < 7) {
      System.out.println(
          "Usage: Please provide arguments: inputFilePath,hasHeader,timeIdx,valueIdx,N,m,errorType,(outDir)");
    }
    String input = args[0];
    boolean hasHeader = Boolean.parseBoolean(args[1]);
    int timeIdx = Integer.parseInt(args[2]);
    int valueIdx = Integer.parseInt(args[3]);
    int N = Integer.parseInt(args[4]); // N<=0表示读全部行，N>0表示读最多N行
    int m = Integer.parseInt(args[5]);
    String errorTypeStr = args[6];
    DP.ERRORtype errorType;
    if (errorTypeStr.equals("L1")) {
      errorType = DP.ERRORtype.L1;
    } else if (errorTypeStr.equals("L2")) {
      errorType = DP.ERRORtype.L2;
    } else if (errorTypeStr.equals("L_infy")) {
      errorType = DP.ERRORtype.L_infy;
    } else {
      throw new IOException("please input errorType as L1/L2/L_infy");
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
    System.out.println("errorType: " + errorType);
    System.out.println("outDir: " + outDir);

    // 读取原始序列
    List<Point> points = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);

    String outputFile =
        generateOutputFileName(input, outDir, "-BU" + errorType + "-n" + points.size() + "-m" + m);
    System.out.println("Output file: " + outputFile); // do not modify this hint string log

    // 采样
    long startTime = System.currentTimeMillis();
    //    List<Point> results = SWAB.seg_bottomUp_m_withTimestamps(points, m, null, false);
    List<Point> results = BottomUpYdiff.reducePoints(points, m, errorType, false);
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

    // 输出结果到csv，按照z,x,y三列，因为results结果已经按照z（即DS）递增排序，对应bottom-up的淘汰顺序，越小代表越早被淘汰
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
