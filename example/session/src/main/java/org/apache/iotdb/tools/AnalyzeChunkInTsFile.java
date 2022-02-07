package org.apache.iotdb.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class AnalyzeChunkInTsFile {

  //    public static final String TSFILE_AFTER_SKETCH=
  // "D:\\exp\\fullGame1_maxDelay=1s\\TsFile_sketch_view_unseq.txt";
  //    public static final String OUTPUT_FILE = "chunk_analysis_unseq.csv";

  public static void main(String[] args) throws IOException {
    String TSFILE_AFTER_SKETCH = args[0];
    String OUTPUT_FILE = args[1];

    BufferedReader reader = new BufferedReader(new FileReader(TSFILE_AFTER_SKETCH));
    PrintWriter writer = new PrintWriter(new FileWriter(OUTPUT_FILE));

    String line = null;
    boolean toRead = false;

    while ((line = reader.readLine()) != null) {
      if (toRead) {
        String[] tmp1 = line.split("endTime: ");
        long startTime = Long.parseLong(tmp1[0].split("startTime: ")[1].trim());
        long endTime = Long.parseLong(tmp1[1].split("count: ")[0].trim());
        writer.write(startTime + "," + endTime + "\n");
        toRead = false;
      } else if (line.contains("[Chunk] of")) {
        toRead = true;
      }
    }
    reader.close();
    writer.close();
  }
}
