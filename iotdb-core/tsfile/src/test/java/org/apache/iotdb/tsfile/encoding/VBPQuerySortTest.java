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

public class VBPQuerySortTest {

  private static final int[] BLOCK_SIZES = {32, 64, 128, 256, 512, 1024, 2048, 4096, 8192};

  public static int[] querySortSingleBlockByDecode(
      byte[] encodedResult, ArrayList<VBPIndexLong> indexList, int targetBlockId) {
    long[] decoded = VBPIndexLongTest.Decoder(encodedResult, indexList);
    int blockSize = VBPIndexLongTest.bytes2Integer(encodedResult, 4, 4);
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
        long v1 = decoded[indices[j]];
        long v2 = decoded[indices[min]];
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
    String parentDir = "path/to/your/directory/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/";
    String outputPath = outputParentDir + "vbp_query_sort_decode.csv";

    int repeatTime = 100;
    int targetBlockId = 0;
    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Block Size",
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
      String datasetName = VBPIndexLongTest.extractFileName(file.toString());
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
      long[] dataArr = new long[data.size()];
      for (int i = 0; i < data.size(); i++) {
        dataArr[i] = (long) (data.get(i) * maxMul);
      }

      for (int blockSize : BLOCK_SIZES) {
        byte[] encodedResult = new byte[Math.max(16, dataArr.length * 12)];
        int length = 0;
        long start = System.nanoTime();
        for (int repeat = 0; repeat < repeatTime; repeat++) {
          ArrayList<VBPIndexLong> tmpIndexList = new ArrayList<>();
          length = VBPIndexLongTest.Encoder(dataArr, blockSize, tmpIndexList, encodedResult);
        }
        long end = System.nanoTime();
        long encodeTime = (end - start) / repeatTime;
        ArrayList<VBPIndexLong> indexList = new ArrayList<>();
        length = VBPIndexLongTest.Encoder(dataArr, blockSize, indexList, encodedResult);

        int[] sortedIndices = null;
        start = System.nanoTime();
        for (int repeat = 0; repeat < repeatTime; repeat++) {
          sortedIndices = querySortSingleBlockByDecode(encodedResult, indexList, targetBlockId);
        }
        end = System.nanoTime();
        long queryTime = (end - start) / repeatTime;
        System.out.println(
            "blockSize="
                + blockSize
                + ", sortedCount: "
                + (sortedIndices == null ? 0 : sortedIndices.length));

        double compressionRatio = length / (double) (Math.max(1, data.size()) * Long.BYTES);
        writer.writeRecord(
            new String[] {
              datasetName,
              "VBP",
              String.valueOf(blockSize),
              String.valueOf(encodeTime),
              String.valueOf(queryTime),
              String.valueOf(data.size()),
              String.valueOf(length),
              String.valueOf(compressionRatio)
            });
      }
    }
    writer.close();
  }
}
