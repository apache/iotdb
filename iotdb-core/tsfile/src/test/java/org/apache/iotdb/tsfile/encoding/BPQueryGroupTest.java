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

public class BPQueryGroupTest {

  public static int[] queryGroupMaxIndexByDecode(byte[] encodedResult, int windowSize) {
    int[] decoded = BPTest.Decoder(encodedResult);
    int groupCount = (decoded.length + windowSize - 1) / windowSize;
    int[] result = new int[groupCount];
    for (int g = 0; g < groupCount; g++) {
      int start = g * windowSize;
      int end = Math.min(decoded.length, start + windowSize);
      int bestIndex = start;
      int bestValue = decoded[start];
      for (int i = start + 1; i < end; i++) {
        if (decoded[i] > bestValue) {
          bestValue = decoded[i];
          bestIndex = i;
        }
      }
      result[g] = bestIndex;
    }
    return result;
  }

  @Test
  public void test0() throws IOException {
    String parentDir = "D://github/xjz17/subcolumn/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/";
    String outputPath = outputParentDir + "bp_query_group_max.csv";

    int blockSize = 512;
    int repeatTime = 100;
    int windowSize = 30;

    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Encoding Time",
          "Decoding Time",
          "Points",
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
        int curDecimal = BPTest.getDecimalPrecision(fStr);
        if (curDecimal > maxDecimal) {
          maxDecimal = curDecimal;
        }
        data.add(Float.valueOf(fStr));
      }
      inputStream.close();

      int maxMul = (int) Math.pow(10, Math.min(maxDecimal, 8));
      int[] dataArr = new int[data.size()];
      for (int i = 0; i < data.size(); i++) {
        dataArr[i] = (int) (data.get(i) * maxMul);
      }

      byte[] encodedResult = new byte[Math.max(16, dataArr.length * 8)];
      int length = 0;
      long start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        length = BPTest.Encoder(dataArr, blockSize, encodedResult);
      }
      long end = System.nanoTime();
      long encodeTime = (end - start) / repeatTime;

      int[] groupResult = null;
      start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        groupResult = queryGroupMaxIndexByDecode(encodedResult, windowSize);
      }
      end = System.nanoTime();
      long queryTime = (end - start) / repeatTime;
      System.out.println("groupCount: " + (groupResult == null ? 0 : groupResult.length));

      double compressionRatio = length / (double) (Math.max(1, data.size()) * Long.BYTES);
      writer.writeRecord(
          new String[] {
            datasetName,
            "BP",
            String.valueOf(encodeTime),
            String.valueOf(queryTime),
            String.valueOf(data.size()),
            String.valueOf(length),
            String.valueOf(compressionRatio)
          });
    }
    writer.close();
  }
}
