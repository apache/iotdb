package org.apache.iotdb.writeData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

// java ExtractFullGameData /data3/raw_data/data/full-game full-game-small 1 5 495760
public class ExtractFullGameData {

  public static void main(String[] args) throws IOException {
    String inPath = args[0];
    String outPath = args[1];
    int timeIdx = Integer.parseInt(args[2]);
    int valueIdx = Integer.parseInt(args[3]);
    int lineNum = Integer.parseInt(args[4]); // -1 means no constraint

    File f = new File(inPath);
    FileWriter fileWriter = new FileWriter(outPath);
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    PrintWriter printWriter = new PrintWriter(fileWriter);
    int cnt = 0;
    while (((lineNum > 0 && cnt < lineNum) || lineNum < 0) && (line = reader.readLine()) != null) {
      String[] split = line.split(",");
      long timestamp = Long.parseLong(split[timeIdx]);
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
