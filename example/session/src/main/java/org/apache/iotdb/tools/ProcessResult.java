package org.apache.iotdb.tools;

import java.io.*;

public class ProcessResult {

  public static void main(String[] args) throws IOException {

    String inFilePath = args[0];
    String outFilePath = args[1];
    String sumOutFilePath = args[2];

    BufferedReader reader = new BufferedReader(new FileReader(inFilePath));
    FileWriter writer = new FileWriter(outFilePath);
    FileWriter sumWriter = new FileWriter(sumOutFilePath, true);
    String readLine = null;
    boolean firstTime = true;
    int metaNum = 0, dataNum = 0, readMemChunkNum = 0;
    long metaTime = 0;
    long dataTime = 0;
    long totalTime = 0;
    long readMemChunkTime = 0;
    int counter = 0;
    while ((readLine = reader.readLine()) != null) {
      if (readLine.startsWith("select")) {
        String[] values = readLine.split("\t");
        if (firstTime) {
          metaNum = Integer.parseInt(values[4]);
          dataNum = Integer.parseInt(values[8]);
          readMemChunkNum = Integer.parseInt(values[12]);
        }
        metaTime += Long.parseLong(values[2]);
        dataTime += Long.parseLong(values[6]);
        readMemChunkTime += Long.parseLong(values[10]);
        totalTime += Long.parseLong(values[14]);
        counter++;
        writer.write(readLine + "\n");
      }
    }

    writer.write(
        "meta_num\t avg_meta\t data_num\t avg_data\t read_mem_chunk_num\t avg_read_mem_chunk_time\t avg_total\n"
            + metaNum
            + "\t"
            + (double) metaTime / 1000000 / counter
            + "\t"
            + dataNum
            + "\t"
            + (double) dataTime / 1000000 / counter
            + "\t"
            + readMemChunkNum
            + "\t"
            + (double) readMemChunkTime / 1000000 / counter
            + "\t"
            + (double) totalTime / 1000000 / counter);

    sumWriter.write(
        metaNum
            + ","
            + (double) metaTime / 1000000 / counter
            + ","
            + dataNum
            + ","
            + (double) dataTime / 1000000 / counter
            + ","
            + readMemChunkNum
            + ","
            + (double) readMemChunkTime / 1000000 / counter
            + ","
            + (double) totalTime / 1000000 / counter
            + "\n");

    reader.close();
    writer.close();
    sumWriter.close();
  }
}
