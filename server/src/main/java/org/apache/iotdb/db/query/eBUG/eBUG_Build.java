package org.apache.iotdb.db.query.eBUG;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.iotdb.db.query.eBUG.eBUG.buildEffectiveArea;

public class eBUG_Build {
  // 输入一条时间序列 t,v
  // 输出按照bottom-up淘汰顺序排列的dominated significance,t,v。
  // 用于后期在线采样时选取倒数m个点（也就是DS最大的m个点，或者最晚淘汰的m个点）作为采样结果（选出之后要自行把这m个点重新按照时间戳x递增排列）
  public static void main(String[] args) {
    if (args.length < 5) {
      System.out.println(
          "Usage: Please provide arguments: inputFilePath,hasHeader,timeIdx,valueIdx,eParam,(outDir)");
    }
    String input = args[0];
    boolean hasHeader = Boolean.parseBoolean(args[1]);
    int timeIdx = Integer.parseInt(args[2]);
    int valueIdx = Integer.parseInt(args[3]);
    int eParam = Integer.parseInt(args[4]);
    String outDir;
    if (args.length > 5) {
      outDir = args[5]; // 表示输出文件保存到指定的文件夹
    } else {
      outDir = null; // 表示输出文件保存到input文件所在的文件夹
    }
    String outputFile = generateOutputFileName(input, eParam, outDir);

    // 打印信息供用户确认
    System.out.println("Input file: " + input);
    System.out.println("Has header: " + hasHeader);
    System.out.println("Time index: " + timeIdx);
    System.out.println("Value index: " + valueIdx);
    System.out.println("eParam: " + eParam);
    System.out.println("outDir: " + outDir);
    System.out.println("Output file: " + outputFile);

    // 开始运算
    Polyline polyline = new Polyline();

    // Using Files.lines() for memory-efficient line-by-line reading
    try (Stream<String> lines = Files.lines(Paths.get(input))) {
      Iterator<String> iterator = lines.iterator();
      int lineNumber = 0;

      // Skip the header line if necessary
      if (hasHeader && iterator.hasNext()) {
        iterator.next(); // Skip the header
      }

      // Process each line
      while (iterator.hasNext()) {
        lineNumber++;
        String line = iterator.next();
        String[] columns = line.split(",");

        if (columns.length > Math.max(timeIdx, valueIdx)) {
          double time;
          if (timeIdx < 0) {
            time = lineNumber;
          } else {
            time = Double.parseDouble(columns[timeIdx]);
          }
          double value = Double.parseDouble(columns[valueIdx]);
          polyline.addVertex(new Point(time, value));
          //                    System.out.println("Line " + lineNumber + " - Time: " + time + ",
          // Value: " + value);
        } else {
          System.out.println("Line " + lineNumber + " is malformed (not enough columns).");
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    long startTime = System.currentTimeMillis();
    List<Point> results = buildEffectiveArea(polyline, eParam, false); // precomputation mode
    long endTime = System.currentTimeMillis();

    //        System.out.println("result point number: " + results.size()); // precomputation
    // mode所以自然是等于n个点
    System.out.println(
        "n="
            + polyline.getVertices().size()
            + ", e="
            + eParam
            + ", Time taken to reduce points: "
            + (endTime - startTime)
            + "ms");

    // 输出结果到csv，按照z,x,y三列，因为results结果已经按照z（即DS）递增排序，对应bottom-up的淘汰顺序，越小代表越早被淘汰
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
      // 写入表头
      writer.write("z,x,y");
      writer.newLine();

      // 写入数据行，按顺序 z, x, y
      for (Point point : results) {
        writer.write(point.z + "," + point.x + "," + point.y);
        writer.newLine();
      }

      System.out.println("CSV file written successfully to " + outputFile);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // Method to generate the output file name based on input path
  private static String generateOutputFileName(String inputFilePath, int e, String outDir) {
    // Get the file name without extension
    Path path = Paths.get(inputFilePath);
    String fileNameWithoutExtension = path.getFileName().toString();
    String name = fileNameWithoutExtension.substring(0, fileNameWithoutExtension.lastIndexOf('.'));

    // Get the file extension
    String extension =
        path.getFileName().toString().substring(fileNameWithoutExtension.lastIndexOf('.'));

    // Create output file path by appending '-ds' before the extension
    String outputFile = name + "-ds-e" + e + extension;

    if (outDir == null) { // 表示使用input文件所在的文件夹
      // Handle path compatibility for different operating systems (Linux/Windows)
      return path.getParent().resolve(outputFile).toString();
    } else {
      Path out = Paths.get(outDir);
      return out.resolve(outputFile).toString();
    }
  }
}
