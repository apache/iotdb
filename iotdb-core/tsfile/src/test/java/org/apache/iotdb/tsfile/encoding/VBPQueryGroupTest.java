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

public class VBPQueryGroupTest {

  private static final int[] BLOCK_SIZES = {32, 64, 128, 256, 512, 1024, 2048, 4096, 8192};
  private static final int TARGET_GROUP_COUNT = 20;

  private static class RangeGroupConfig {
    long start;
    long width;
    int groupCount;
  }

  public static int[] queryGroupCountByValueRangeByDecode(
      byte[] encodedResult,
      ArrayList<VBPIndexLong> indexList,
      long rangeStart,
      long rangeWidth,
      int bucketCount) {
    long[] decoded = VBPIndexLongTest.Decoder(encodedResult, indexList);
    int[] result = new int[bucketCount];
    for (long value : decoded) {
      long bucket = Math.floorDiv(value - rangeStart, rangeWidth);
      if (bucket >= 0 && bucket < bucketCount) {
        result[(int) bucket]++;
      }
    }
    return result;
  }

  private static RangeGroupConfig buildRangeGroupConfig(long[] dataArr) {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (long value : dataArr) {
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
    }

    long span = max - min + 1L;
    long width = Math.max(1L, (span + TARGET_GROUP_COUNT - 1L) / TARGET_GROUP_COUNT);
    // long nice = niceWidth(width);
    // Keep bucket count as close as possible to TARGET_GROUP_COUNT.
    int groupCount = TARGET_GROUP_COUNT;

    RangeGroupConfig config = new RangeGroupConfig();
    config.start = min;
    config.width = width;
    config.groupCount = groupCount;
    return config;
  }

  /*
  private static long niceWidth(long rawWidth) {
    long scale = 1L;
    while (rawWidth >= 10L) {
      rawWidth = (rawWidth + 9L) / 10L;
      scale *= 10L;
    }
    if (rawWidth <= 1L) {
      return scale;
    }
    if (rawWidth <= 2L) {
      return 2L * scale;
    }
    if (rawWidth <= 5L) {
      return 5L * scale;
    }
    return 10L * scale;
  }
  */

  @Test
  public void test0() throws IOException {
    String parentDir = "path/to/your/directory/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/";
    String outputPath = outputParentDir + "vbp_query_group_range_count.csv";

    int repeatTime = 100;
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
      RangeGroupConfig groupConfig = buildRangeGroupConfig(dataArr);
      System.out.println(
          "group range config: start="
              + groupConfig.start
              + ", width="
              + groupConfig.width
              + ", groupCount="
              + groupConfig.groupCount);

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

        int[] groupResult = null;
        start = System.nanoTime();
        for (int repeat = 0; repeat < repeatTime; repeat++) {
          groupResult =
              queryGroupCountByValueRangeByDecode(
                  encodedResult,
                  indexList,
                  groupConfig.start,
                  groupConfig.width,
                  groupConfig.groupCount);
        }
        end = System.nanoTime();
        long queryTime = (end - start) / repeatTime;
        System.out.println(
            "blockSize=" + blockSize + ", groupCount: " + (groupResult == null ? 0 : groupResult.length));

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
