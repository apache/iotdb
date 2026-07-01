package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

public class BPUpdateAppendTest {

  private static void int2Bytes(int value, int pos, byte[] out) {
    out[pos] = (byte) (value >> 24);
    out[pos + 1] = (byte) (value >> 16);
    out[pos + 2] = (byte) (value >> 8);
    out[pos + 3] = (byte) value;
  }

  private static int skipBlock(byte[] encodedResult, int encodePos, int rowCount) {
    encodePos += 4;
    int[] tmp = new int[rowCount];
    return BPTest.BPDecoder(encodedResult, encodePos, tmp);
  }

  @Test
  public void test0() throws IOException {
    String parentDir = "D://github/xjz17/subcolumn/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/update/";
    String outputPath = outputParentDir + "bp_update_append.csv";
    int blockSize = 512;
    int repeatTime = 200;

    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Encoding Time",
          "Append-only Time with Sub-column",
          "Points",
          "Remaining Points",
          "Compressed Size",
          "Compression Ratio"
        });

    File directory = new File(inputParentDir);
    File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
    if (csvFiles == null) {
      writer.close();
      return;
    }
    for (File file : csvFiles) {
      String datasetName = BPTest.extractFileName(file.toString());
      if (datasetName.equals("POI-lon") || datasetName.equals("POI-lat")) {
        continue;
      }
      InputStream inputStream = Files.newInputStream(file.toPath());
      CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
      ArrayList<Float> data = new ArrayList<>();
      int maxDecimal = 0;
      while (loader.readRecord()) {
        String fStr = loader.getValues()[0];
        if (fStr.isEmpty()) continue;
        int d = BPTest.getDecimalPrecision(fStr);
        if (d > maxDecimal) maxDecimal = d;
        data.add(Float.valueOf(fStr));
      }
      inputStream.close();
      int mul = (int) Math.pow(10, Math.min(maxDecimal, 8));
      int[] origin = new int[data.size()];
      int maxValue = Integer.MIN_VALUE;
      for (int i = 0; i < data.size(); i++) {
        origin[i] = (int) (data.get(i) * mul);
        if (origin[i] > maxValue) maxValue = origin[i];
      }
      if (origin.length == 0) continue;

      int[] appended = new int[origin.length + 1];
      System.arraycopy(origin, 0, appended, 0, origin.length);
      appended[origin.length] = (maxValue == Integer.MAX_VALUE) ? maxValue : maxValue + 1;

      byte[] encodedResult = new byte[Math.max(16, appended.length * 8)];
      int length = 0;
      long s = System.nanoTime();
      for (int r = 0; r < repeatTime; r++) length = BPTest.Encoder(origin, blockSize, encodedResult);
      long e = System.nanoTime();
      long encodeTime = (e - s) / repeatTime;

      int numBlocks = origin.length / blockSize;
      int remainder = origin.length % blockSize;
      if (remainder <= 0) continue;
      int tailStart = 8;
      for (int i = 0; i < numBlocks; i++) tailStart = skipBlock(encodedResult, tailStart, blockSize);
      int newRemainder = remainder + 1;
      int updatedLength = length;
      s = System.nanoTime();
      for (int r = 0; r < repeatTime; r++) {
        int pos = tailStart;
        if (newRemainder <= 3) {
          int base = numBlocks * blockSize;
          for (int i = 0; i < newRemainder; i++) {
            int2Bytes(appended[base + i], pos, encodedResult);
            pos += 4;
          }
        } else {
          pos = BPTest.BlockEncoder(appended, numBlocks, blockSize, newRemainder, pos, encodedResult);
        }
        updatedLength = pos;
      }
      e = System.nanoTime();
      long appendTime = (e - s) / repeatTime;

      double ratio = length / (double) (Math.max(1, origin.length) * Long.BYTES);
      writer.writeRecord(new String[] {datasetName, "BP", String.valueOf(encodeTime), String.valueOf(appendTime),
          String.valueOf(origin.length), String.valueOf(remainder), String.valueOf(updatedLength), String.valueOf(ratio)});
    }
    writer.close();
  }
}
