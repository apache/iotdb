package org.apache.iotdb.db.query.eBUG;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.apache.iotdb.db.query.eBUG.Tool.generateOutputFileName;
import static org.apache.iotdb.db.query.eBUG.eBUG.buildEffectiveArea;

public class sample_eBUG {
  // 输入一条时间序列 t,v
  // 输出按照bottom-up淘汰顺序倒序排列的dominated significance,t,v。
  // 用于后期在线采样时选取前m个点（也就是DS最大的m个点，或者最晚淘汰的m个点）作为采样结果（选出之后要自行把这m个点重新按照时间戳x递增排列）
  public static void main(String[] args) {
    if (args.length < 7) {
      System.out.println(
          "Usage: Please provide arguments: inputFilePath,hasHeader,timeIdx,valueIdx,N,m,eParam,(outDir)");
    }
    String input = args[0];
    boolean hasHeader = Boolean.parseBoolean(args[1]);
    int timeIdx = Integer.parseInt(args[2]);
    int valueIdx = Integer.parseInt(args[3]);
    int N = Integer.parseInt(args[4]); // N<=0表示读全部行，N>0表示读最多N行
    int m = Integer.parseInt(args[5]); // m<=2代表预计算全部点，m>2代表在线采样m个点
    int eParam = Integer.parseInt(args[6]);
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
    System.out.println("eParam: " + eParam);
    System.out.println("outDir: " + outDir);

    // 读取原始序列
    List<Point> points = Tool.readFromFile(input, hasHeader, timeIdx, valueIdx, N);

    String outputFile =
        generateOutputFileName(input, outDir, "-eBUG-e" + eParam + "-n" + points.size() + "-m" + m);
    System.out.println("Output file: " + outputFile);

    long startTime = System.currentTimeMillis();
    // m<=2代表预计算全部点，m>2代表在线采样m个点
    List<Point> results = buildEffectiveArea(points, eParam, false, m);
    long endTime = System.currentTimeMillis();

    System.out.println("result point number: " + results.size());
    System.out.println(
        "n="
            + points.size()
            + ", m="
            + m
            + ", e="
            + eParam
            + ", Time taken to reduce points: "
            + (endTime - startTime)
            + "ms");

    // 输出结果到csv
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
      // 写入表头
      if (m <= 2) {
        System.out.println(
            "precomputation mode, outputting (id,x,y,z) ordered by z in descending order");
        writer.write("id,x,y,z");
        writer.newLine();

        // 写入数据行，
        // 按照idx,x,y,z四列，results结果已经按照z（即DS）递减排序，对应bottom-up的淘汰顺序的倒序，越大代表越晚被淘汰
        // idx是从1开始的顺序编号
        int idx = 1;
        for (Point point : results) {
          writer.write(idx + "," + point.x + "," + point.y + "," + point.z);
          writer.newLine();
          idx++;
        }
      } else {
        System.out.println(
            "online sampling mode, outputting (x,y) ordered by x in ascending order");
        writer.write("x,y");
        writer.newLine();

        // 写入数据行，按顺序 x,y
        for (Point point : results) {
          writer.write(point.x + "," + point.y);
          writer.newLine();
        }
      }

      System.out.println("CSV file written successfully to " + outputFile);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
