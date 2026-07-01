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
import java.util.Arrays;

public class SubcolumnAddDictQueryGroupTest {

  private static final int[] BLOCK_SIZES = {32, 64, 128, 256, 512, 1024, 2048, 4096, 8192};
  private static final int TARGET_GROUP_COUNT = 20;

  private static class BlockMeta {
    int minDelta;
    int m;
    int beta;
    int l;
    int[] bitWidthList;
    int[] encodingType;
    int[] segmentPos;
    int[] runCountList;
    int[] cardinalityList;
    int[][] rleRunEndList;
    int[][] rleValueList;
    int[][] dictKeyList;
    int[] dictBitWidthList;
    int[] dictIndexPosList;
    int nextPos;
  }

  private static class RangeGroupConfig {
    int start;
    int width;
    int groupCount;
  }

  public static int[] queryGroupCountByValueRange(
      byte[] encodedResult, int rangeStart, int rangeWidth, int bucketCount) {
    int encodePos = 0;
    int dataLength = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;
    int blockSize = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;

    int numBlocks = dataLength / blockSize;
    int remainder = dataLength % blockSize;
    int encodedBlocks = numBlocks + (remainder > 3 ? 1 : 0);

    BlockMeta[] metas = new BlockMeta[encodedBlocks];
    int[] blockRowCount = new int[encodedBlocks];
    for (int i = 0; i < encodedBlocks; i++) {
      int rowCount = (i < numBlocks) ? blockSize : remainder;
      if (rowCount == 0) {
        break;
      }
      blockRowCount[i] = rowCount;
      BlockMeta meta = parseBlockMeta(encodedResult, encodePos, blockSize, rowCount);
      metas[i] = meta;
      encodePos = meta.nextPos;
    }

    int[] groupCounts = new int[bucketCount];
    long rangeEnd = rangeStart + (long) rangeWidth * bucketCount;
    for (int b = 0; b < encodedBlocks; b++) {
      accumulateBlockRangeCounts(
          encodedResult,
          metas[b],
          blockRowCount[b],
          rangeStart,
          rangeWidth,
          bucketCount,
          rangeEnd,
          groupCounts);
    }

    if (remainder > 0 && remainder <= 3) {
      for (int i = 0; i < remainder; i++) {
        int value = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
        encodePos += 4;
        int bucket = bucketIndex(value, rangeStart, rangeWidth, bucketCount);
        if (bucket >= 0) {
          groupCounts[bucket]++;
        }
      }
    }

    return groupCounts;
  }

  private static void accumulateBlockRangeCounts(
      byte[] encodedResult,
      BlockMeta meta,
      int rowCount,
      int rangeStart,
      int rangeWidth,
      int bucketCount,
      long rangeEnd,
      int[] groupCounts) {
    if (rowCount <= 0) {
      return;
    }
    if (meta.m == 0) {
      int bucket = bucketIndex(meta.minDelta, rangeStart, rangeWidth, bucketCount);
      if (bucket >= 0) {
        groupCounts[bucket] += rowCount;
      }
      return;
    }

    int[] candidates = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      candidates[i] = i;
    }

    accumulateRangeByPrefix(
        encodedResult,
        meta,
        rangeStart,
        rangeWidth,
        bucketCount,
        rangeEnd,
        groupCounts,
        candidates,
        rowCount,
        meta.l - 1,
        0L);
  }

  private static void accumulateRangeByPrefix(
      byte[] encodedResult,
      BlockMeta meta,
      int rangeStart,
      int rangeWidth,
      int bucketCount,
      long rangeEnd,
      int[] groupCounts,
      int[] candidates,
      int candidateLength,
      int level,
      long prefix) {
    if (candidateLength <= 0) {
      return;
    }

    int remainingBits = Math.max(0, level * meta.beta);
    long prefixBase = prefix << remainingBits;
    long suffixMax = (remainingBits == 0) ? 0L : ((1L << remainingBits) - 1L);
    long lowValue = meta.minDelta + prefixBase;
    long highValue = meta.minDelta + prefixBase + suffixMax;

    if (highValue < rangeStart || lowValue >= rangeEnd) {
      return;
    }

    int startBucket = (int) Math.floorDiv(lowValue - (long) rangeStart, rangeWidth);
    int endBucket = (int) Math.floorDiv(highValue - (long) rangeStart, rangeWidth);
    if (startBucket == endBucket && startBucket >= 0 && startBucket < bucketCount) {
      groupCounts[startBucket] += candidateLength;
      return;
    }

    if (level < 0) {
      int bucket = bucketIndex(lowValue, rangeStart, rangeWidth, bucketCount);
      if (bucket >= 0) {
        groupCounts[bucket] += candidateLength;
      }
      return;
    }

    int radix = 1 << meta.beta;
    int[] partCounts = new int[radix];
    int[] partsPerCandidate = new int[candidateLength];
    for (int i = 0; i < candidateLength; i++) {
      int part = partValueAtIndex(encodedResult, meta, level, candidates[i]);
      partsPerCandidate[i] = part;
      partCounts[part]++;
    }

    int[][] partCandidates = new int[radix][];
    for (int part = 0; part < radix; part++) {
      if (partCounts[part] > 0) {
        partCandidates[part] = new int[partCounts[part]];
      }
    }

    int[] offsets = new int[radix];
    for (int i = 0; i < candidateLength; i++) {
      int part = partsPerCandidate[i];
      partCandidates[part][offsets[part]++] = candidates[i];
    }

    for (int part = 0; part < radix; part++) {
      int length = partCounts[part];
      if (length == 0) {
        continue;
      }
      long childPrefix = (prefix << meta.beta) | part;
      accumulateRangeByPrefix(
          encodedResult,
          meta,
          rangeStart,
          rangeWidth,
          bucketCount,
          rangeEnd,
          groupCounts,
          partCandidates[part],
          length,
          level - 1,
          childPrefix);
    }
  }

  private static int partValueAtIndex(
      byte[] encodedResult, BlockMeta meta, int level, int localIndex) {
    int type = meta.encodingType[level];
    int currentBitWidth = meta.bitWidthList[level];
    if (type == 0) {
      long bitStart = ((long) meta.segmentPos[level]) * 8L;
      return SubcolumnPruneNewTest.bytesToInt(
          encodedResult, (int) (bitStart + (long) localIndex * currentBitWidth), currentBitWidth);
    }
    if (type == 1) {
      int[] runEnd = meta.rleRunEndList[level];
      int[] rleValues = meta.rleValueList[level];
      int runIndex = Arrays.binarySearch(runEnd, localIndex + 1);
      if (runIndex < 0) {
        runIndex = -runIndex - 1;
      }
      if (runIndex < 0 || runIndex >= rleValues.length) {
        return 0;
      }
      return rleValues[runIndex];
    }
    int dictIndex =
        bitPackedValueAt(
            encodedResult, meta.dictIndexPosList[level], meta.dictBitWidthList[level], localIndex);
    return meta.dictKeyList[level][dictIndex];
  }

  private static int bucketIndex(long value, int rangeStart, int rangeWidth, int bucketCount) {
    long idx = Math.floorDiv(value - (long) rangeStart, rangeWidth);
    if (idx < 0 || idx >= bucketCount) {
      return -1;
    }
    return (int) idx;
  }

  private static RangeGroupConfig buildRangeGroupConfig(int[] dataArr) {
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    for (int value : dataArr) {
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
    }

    long range = (long) max - min + 1L;
    int width = (int) Math.max(1L, (range + TARGET_GROUP_COUNT - 1L) / TARGET_GROUP_COUNT);
    // int nice = niceWidth(width);
    // Keep bucket count as close as possible to TARGET_GROUP_COUNT.
    int groupCount = TARGET_GROUP_COUNT;

    RangeGroupConfig config = new RangeGroupConfig();
    config.start = min;
    config.width = width;
    config.groupCount = groupCount;
    return config;
  }

  /*
  private static int niceWidth(int rawWidth) {
    int scale = 1;
    while (rawWidth >= 10) {
      rawWidth = (rawWidth + 9) / 10;
      scale *= 10;
    }
    if (rawWidth <= 1) {
      return scale;
    }
    if (rawWidth <= 2) {
      return 2 * scale;
    }
    if (rawWidth <= 5) {
      return 5 * scale;
    }
    return 10 * scale;
  }
  */

  private static BlockMeta parseBlockMeta(byte[] encodedResult, int encodePos, int blockSize, int rowCount) {
    BlockMeta meta = new BlockMeta();
    meta.minDelta = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;
    meta.m = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 1);
    encodePos += 1;

    if (meta.m == 0) {
      meta.nextPos = encodePos;
      return meta;
    }

    meta.beta = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 1);
    encodePos += 1;
    meta.l = (meta.m + meta.beta - 1) / meta.beta;

    meta.bitWidthList = new int[meta.l];
    encodePos =
        SubcolumnPruneNewTest.decodeBitPacking(
            encodedResult, encodePos, 8, meta.l, meta.bitWidthList);
    meta.encodingType = new int[meta.l];
    encodePos =
        SubcolumnPruneNewTest.decodeBitPacking(
            encodedResult, encodePos, 2, meta.l, meta.encodingType);

    int bw = SubcolumnPruneNewTest.bitWidth(blockSize);
    meta.segmentPos = new int[meta.l];
    meta.runCountList = new int[meta.l];
    meta.cardinalityList = new int[meta.l];
    meta.rleRunEndList = new int[meta.l][];
    meta.rleValueList = new int[meta.l][];
    meta.dictKeyList = new int[meta.l][];
    meta.dictBitWidthList = new int[meta.l];
    meta.dictIndexPosList = new int[meta.l];
    int scanPos = encodePos;

    for (int i = 0; i < meta.l; i++) {
      meta.segmentPos[i] = scanPos;
      int type = meta.encodingType[i];
      int currentBitWidth = meta.bitWidthList[i];
      if (type == 0) {
        long bitPos = ((long) scanPos) * 8L + (long) currentBitWidth * rowCount;
        scanPos = (int) ((bitPos + 7L) / 8L);
      } else if (type == 1) {
        int runCount = ((encodedResult[scanPos] & 0xFF) << 8) | (encodedResult[scanPos + 1] & 0xFF);
        meta.runCountList[i] = runCount;
        scanPos += 2;
        int[] runEnd = new int[runCount];
        scanPos = SubcolumnPruneNewTest.decodeBitPacking(encodedResult, scanPos, bw, runCount, runEnd);
        int[] rleValues = new int[runCount];
        scanPos =
            SubcolumnPruneNewTest.decodeBitPacking(
                encodedResult, scanPos, currentBitWidth, runCount, rleValues);
        meta.rleRunEndList[i] = runEnd;
        meta.rleValueList[i] = rleValues;
      } else {
        int cardinality = ((encodedResult[scanPos] & 0xFF) << 8) | (encodedResult[scanPos + 1] & 0xFF);
        meta.cardinalityList[i] = cardinality;
        scanPos += 2;
        int[] dictKey = new int[cardinality];
        int dictIndexPos =
            SubcolumnPruneNewTest.decodeBitPacking(
                encodedResult, scanPos, currentBitWidth, cardinality, dictKey);
        int dictBitWidth = SubcolumnPruneNewTest.bitWidth(cardinality);
        long bitPos = ((long) dictIndexPos) * 8L + (long) rowCount * dictBitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
        meta.dictKeyList[i] = dictKey;
        meta.dictBitWidthList[i] = dictBitWidth;
        meta.dictIndexPosList[i] = dictIndexPos;
      }
    }

    meta.nextPos = scanPos;
    return meta;
  }

  private static int bitPackedValueAt(
      byte[] encodedResult, int bitPackedStartBytePos, int bitWidth, int index) {
    long bitPos = ((long) bitPackedStartBytePos) * 8L + (long) index * bitWidth;
    return SubcolumnPruneNewTest.bytesToInt(encodedResult, (int) bitPos, bitWidth);
  }

  public static int getDecimalPrecision(String str) {
    int decimalIndex = str.indexOf('.');
    if (decimalIndex == -1) {
      return 0;
    }
    return str.length() - decimalIndex - 1;
  }

  public static String extractFileName(String path) {
    if (path == null || path.isEmpty()) {
      return "";
    }
    File file = new File(path);
    String fileName = file.getName();
    int dotIndex = fileName.lastIndexOf('.');
    if (dotIndex == -1 || dotIndex == 0) {
      return fileName;
    }
    return fileName.substring(0, dotIndex);
  }

  @Test
  public void test0() throws IOException {
    String parentDir = "path/to/your/directory/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/";
    String outputPath = outputParentDir + "subcolumn_adddict_prunenew_query_group_range_count.csv";

    int repeatTime = 100;
    System.out.println("Output: " + outputPath);
    System.out.println("Block sizes: " + java.util.Arrays.toString(BLOCK_SIZES));
    System.out.println("Repeat time: " + repeatTime);
    System.out.println("Target group count: " + TARGET_GROUP_COUNT);

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
      String datasetName = extractFileName(file.toString());
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
        int currentDecimal = getDecimalPrecision(fStr);
        if (currentDecimal > maxDecimal) {
          maxDecimal = currentDecimal;
        }
        data.add(Float.valueOf(fStr));
      }
      inputStream.close();

      if (maxDecimal > 8) {
        maxDecimal = 8;
      }
      System.out.println("maxDecimal: " + maxDecimal);

      int[] dataArr = new int[data.size()];
      int maxMul = (int) Math.pow(10, maxDecimal);
      for (int i = 0; i < data.size(); i++) {
        dataArr[i] = (int) (data.get(i) * maxMul);
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
        byte[] encodedResult = new byte[dataArr.length * 8];
        int length = 0;

        long start = System.nanoTime();
        for (int repeat = 0; repeat < repeatTime; repeat++) {
          length = SubcolumnPruneNewTest.Encoder(dataArr, blockSize, encodedResult);
        }
        long end = System.nanoTime();
        long encodeTime = (end - start) / repeatTime;

        System.out.println("RangeGroupCountQuery");
        int[] groupCounts = null;
        start = System.nanoTime();
        for (int repeat = 0; repeat < repeatTime; repeat++) {
          groupCounts =
              queryGroupCountByValueRange(
                  encodedResult, groupConfig.start, groupConfig.width, groupConfig.groupCount);
        }
        end = System.nanoTime();
        long queryTime = (end - start) / repeatTime;

        System.out.println(
            "blockSize="
                + blockSize
                + ", groupCount: "
                + (groupCounts == null ? 0 : groupCounts.length));
        double compressionRatio = length / (double) (data.size() * Long.BYTES);
        writer.writeRecord(
            new String[] {
              datasetName,
              "SubcolumnAddDictPruneNew",
              String.valueOf(blockSize),
              String.valueOf(encodeTime),
              String.valueOf(queryTime),
              String.valueOf(data.size()),
              String.valueOf(length),
              String.valueOf(compressionRatio)
            });
        System.out.println("compressionRatio: " + compressionRatio);
      }
    }

    writer.close();
  }
}
