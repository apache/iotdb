package org.apache.iotdb.db.query.eBUG;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.db.query.eBUG.Tool.generateOutputFileName;
import static org.apache.iotdb.db.query.eBUG.Tool.getParam;

public class sample_FSW {
  // 输入一条时间序列 t,v
  // 输出按照bottom-up淘汰顺序排列的dominated significance,t,v。
  // 用于后期在线采样时选取倒数m个点（也就是DS最大的m个点，或者最晚淘汰的m个点）作为采样结果（选出之后要自行把这m个点重新按照时间戳x递增排列）
  public static void main(String[] args) throws IOException {
    if (args.length < 7) {
      System.out.println(
          "Usage: Please provide arguments: inputFilePath,hasHeader,timeIdx,valueIdx,N,m,tolerantRatio,(outDir)");
    }
    String input = args[0];
    boolean hasHeader = Boolean.parseBoolean(args[1]);
    int timeIdx = Integer.parseInt(args[2]);
    int valueIdx = Integer.parseInt(args[3]);
    int N = Integer.parseInt(args[4]); // N<=0表示读全部行，N>0表示读最多N行
    int m = Integer.parseInt(args[5]);
    double tolerantRatio = Double.parseDouble(args[6]); // 查找参数epsilon以使得输出采样点数接近m的程度，比如0.01
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
    System.out.println("tolerantRatio: " + tolerantRatio);
    System.out.println("outDir: " + outDir);

    // 读取原始序列
    List<Point> points = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);
    String outputFile =
        generateOutputFileName(input, outDir, "-FSW" + "-n" + points.size() + "-m" + m);
    System.out.println("Output file: " + outputFile);

    // 查找epsilon参数使得采样点数接近m
    double epsilon = getParam(points, m, FSW::reducePoints, m * tolerantRatio);

    // 采样
    long startTime = System.currentTimeMillis();
    List<Point> results = FSW.reducePoints(points, epsilon);
    long endTime = System.currentTimeMillis();

    System.out.println("result point number: " + results.size());
    System.out.println(
        "n="
            + points.size()
            + ", m="
            + m
            + ", Time taken to reduce points: "
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
