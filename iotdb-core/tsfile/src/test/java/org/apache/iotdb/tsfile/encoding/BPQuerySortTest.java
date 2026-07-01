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

public class BPQuerySortTest {

  public static int[] querySortSingleBlockByDecode(byte[] encodedResult, int targetBlockId) {
    int[] decoded = BPTest.Decoder(encodedResult);
    int blockSize = ((encodedResult[4] & 0xFF) << 24)
        | ((encodedResult[5] & 0xFF) << 16)
        | ((encodedResult[6] & 0xFF) << 8)
        | (encodedResult[7] & 0xFF);
    int dataLength = decoded.length;
    int numBlocks = dataLength / blockSize;
    int remainder = dataLength % blockSize;
    int totalBlocks = numBlocks + (remainder > 0 ? 1 : 0);
    if (targetBlockId < 0 || targetBlockId >= totalBlocks) {
      return new int[0];
    }

    int count = targetBlockId < numBlocks ? blockSize : remainder;
    int start = targetBlockId * blockSize;
    int[] indices = new int[count];
    for (int i = 0; i < count; i++) {
      indices[i] = start + i;
    }

    for (int i = 0; i < count - 1; i++) {
      int min = i;
      for (int j = i + 1; j < count; j++) {
        int v1 = decoded[indices[j]];
        int v2 = decoded[indices[min]];
        if (v1 < v2 || (v1 == v2 && indices[j] < indices[min])) {
          min = j;
        }
      }
      int t = indices[i];
      indices[i] = indices[min];
      indices[min] = t;
    }
    return indices;
  }

  @Test
  public void testDecodeSortSingleBlock() throws IOException {
    String parentDir = "D://github/xjz17/subcolumn/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/";
    String outputPath = outputParentDir + "bp_query_sort_decode.csv";

    int blockSize = 512;
    int repeatTime = 100;
    int targetBlockId = 0;
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

      int[] sortedIndices = null;
      start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        sortedIndices = querySortSingleBlockByDecode(encodedResult, targetBlockId);
      }
      end = System.nanoTime();
      long queryTime = (end - start) / repeatTime;
      System.out.println("sortedCount: " + (sortedIndices == null ? 0 : sortedIndices.length));

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
