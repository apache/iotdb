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

public class VBPUpdateInsertSmallerTest {

  @Test
  public void test0() throws IOException {
    String parentDir = "D://github/xjz17/subcolumn/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/update/";
    String outputPath = outputParentDir + "vbp_update_insert_smaller.csv";
    int blockSize = 512;
    int repeatTime = 200;

    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Encoding Time",
          "Insert Time with Sub-column",
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
      String datasetName = VBPIndexLongTest.extractFileName(file.toString());
      if (datasetName.equals("POI-lon") || datasetName.equals("POI-lat")) {
        continue;
      }
      System.out.println(datasetName);

      InputStream inputStream = Files.newInputStream(file.toPath());
      CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
      ArrayList<Float> data = new ArrayList<>();
      int maxDecimal = 0;
      while (loader.readRecord()) {
        String fStr = loader.getValues()[0];
        if (fStr.isEmpty()) {
          continue;
        }
        int curDecimal = VBPIndexLongTest.getDecimalPrecision(fStr);
        if (curDecimal > maxDecimal) {
          maxDecimal = curDecimal;
        }
        data.add(Float.valueOf(fStr));
      }
      inputStream.close();

      int maxMul = (int) Math.pow(10, Math.min(maxDecimal, 8));
      long[] origin = new long[data.size()];
      long minValue = Long.MAX_VALUE;
      for (int i = 0; i < data.size(); i++) {
        origin[i] = (long) (data.get(i) * maxMul);
        if (origin[i] < minValue) {
          minValue = origin[i];
        }
      }
      if (origin.length == 0) {
        continue;
      }
      long insertValue = (minValue == Long.MIN_VALUE) ? minValue : (minValue - 1);
      long[] inserted = new long[origin.length + 1];
      System.arraycopy(origin, 0, inserted, 0, origin.length);
      inserted[origin.length] = insertValue;

      byte[] encodedResult = new byte[Math.max(16, inserted.length * 12)];
      int encodedLength = 0;
      long start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        encodedLength = VBPIndexLongTest.Encoder(origin, blockSize, new ArrayList<>(), encodedResult);
      }
      long end = System.nanoTime();
      long encodeTime = (end - start) / repeatTime;
      encodedLength = VBPIndexLongTest.Encoder(origin, blockSize, new ArrayList<>(), encodedResult);

      int numBlocks = origin.length / blockSize;
      int remainder = origin.length % blockSize;
      if (remainder <= 0) {
        continue;
      }
      int tailStart = 8 + numBlocks * 12;
      int newRemainder = remainder + 1;

      int updatedLength = encodedLength;
      start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        int encodePos =
            VBPIndexLongTest.BlockEncoder(
                inserted, numBlocks, blockSize, newRemainder, tailStart, new ArrayList<>(), encodedResult);
        updatedLength = encodePos;
      }
      end = System.nanoTime();
      long insertTime = (end - start) / repeatTime;

      double compressionRatio = encodedLength / (double) (origin.length * Long.BYTES);
      writer.writeRecord(
          new String[] {
            datasetName,
            "VBP",
            String.valueOf(encodeTime),
            String.valueOf(insertTime),
            String.valueOf(origin.length),
            String.valueOf(remainder),
            String.valueOf(updatedLength),
            String.valueOf(compressionRatio)
          });
    }
    writer.close();
  }
}
