package org.apache.iotdb.writeData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

// java ExtractDebug _OutOfOrder1_maxdelay300000_mindelay0.data debug.csv 3 7
// 16525359
public class ExtractDebug {

  public static void main(String[] args) throws IOException {
    String inPath = args[0];
    String outPath = args[1];
    int timeIdx = Integer.parseInt(args[2]); // 3
    int valueIdx = Integer.parseInt(args[3]); // 7
    int lineNum = Integer.parseInt(args[4]); // -1 means no constraint

    File f = new File(inPath);
    FileWriter fileWriter = new FileWriter(outPath);
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    PrintWriter printWriter = new PrintWriter(fileWriter);
    int cnt = 0;
    long minTime = -1;
    while (((lineNum > 0 && cnt < lineNum) || lineNum < 0) && (line = reader.readLine()) != null) {
      String[] split = line.split(",");
      long timestamp = Long.parseLong(split[timeIdx]);
      if (minTime == -1) {
        minTime = timestamp;
        timestamp = 0;
      } else {
        timestamp = timestamp - minTime;
      }
      timestamp = (long) (timestamp / 1000);
      long value = Long.parseLong(split[valueIdx]);
      printWriter.print(timestamp);
      printWriter.print(",");
      printWriter.print(value);
      printWriter.println();
      cnt++;
    }
    reader.close();
    printWriter.close();
  }
}
