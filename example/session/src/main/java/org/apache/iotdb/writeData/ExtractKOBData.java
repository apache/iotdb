package org.apache.iotdb.writeData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

// java ExtractTianyuan
// /data3/raw_data/data/KOB/root.kobelco.trans.03.1090001603.2401604.KOB_0002_00_67.csv
// KOB.csv 0 1
public class ExtractKOBData {

  public static void main(String[] args) throws IOException {
    String inPath = args[0];
    String outPath = args[1];
    int timeIdx = Integer.parseInt(args[2]); // 0
    int valueIdx = Integer.parseInt(args[3]); // 1

    File f = new File(inPath);
    FileWriter fileWriter = new FileWriter(outPath);
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    PrintWriter printWriter = new PrintWriter(fileWriter);
    int cnt = 0;
    long lastTimestamp = -1;
    while ((line = reader.readLine()) != null) {
      String[] split = line.split(",");
      long timestamp = Long.parseLong(split[timeIdx]);
      long value = Long.parseLong(split[valueIdx]);
      if (timestamp <= lastTimestamp) {
        System.out.println("out-of-order! " + timestamp + "last: " + lastTimestamp);
        continue;
      }
      printWriter.print(timestamp);
      printWriter.print(",");
      printWriter.print(value);
      printWriter.println();
      cnt++;
      lastTimestamp = timestamp;
    }
    reader.close();
    printWriter.close();
  }
}
