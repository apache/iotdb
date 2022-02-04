package org.apache.iotdb.writeData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class ExtractFullGameData {

  public static void main(String[] args) throws IOException {
    String inPath = args[0];
    String outPath = args[1];
    File f = new File(inPath);
    FileWriter fileWriter = new FileWriter(outPath);
    String line;
    BufferedReader reader = new BufferedReader(new FileReader(f));
    PrintWriter printWriter = new PrintWriter(fileWriter);
    while ((line = reader.readLine()) != null) {
      String[] split = line.split(",");
      long sensorNum = Long.valueOf(split[0]);
      long timestamp = Long.valueOf(split[1]);
      long value = Long.valueOf(split[5]);
      printWriter.print(sensorNum);
      printWriter.print(",");
      printWriter.print(timestamp);
      printWriter.print(",");
      printWriter.print(value);
      printWriter.println();
    }
    reader.close();
    printWriter.close();
  }
}
