package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * 按数据点比例（0%～100%）选取更新点，比较两种块级更新策略：
 *
 * <p>1) 固定策略：不重算最优 beta / 编码类型，复用原块 beta 与原编码类型模板重编码；
 *
 * <p>2) 自适应策略：重算最优 beta / 编码类型（调用 BlockEncoder）。
 */
public class SubcolumnAddDictPruneNewUpdateRatioTest {

  // private static final int[] UPDATE_RATIOS = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
  private static final int[] UPDATE_RATIOS = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20};

  /**
   * If true, updated indices form one contiguous window (centered). At low ratios only a few
   * blocks are dirty, so compression-vs-update-ratio curves (especially re-optimizing α) change
   * gradually from 0% to 1%. If false, legacy uniform spread: ~1% point updates still touch almost
   * every block, which produces a sharp 0–1% drop in plots.
   */
  private static final boolean CLUSTER_UPDATES_IN_CONTIGUOUS_WINDOW = true;

  private static final int STATS_UPDATED_LENGTH = 0;
  private static final int STATS_BETA_CHANGED = 1;
  private static final int STATS_REENCODED_BLOCKS = 2;
  private static final int STATS_MEMCPY_BLOCKS = 3;

  @Test
  public void test0() throws IOException {
    String parentDir = "path/to/your/directory/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/update/";
    String outputPath = outputParentDir + "subcolumn_adddict_prunenew_update_ratio.csv";

    int blockSize = 512;
    int repeatTime = 1000;

    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Update Ratio(%)",
          "Total Blocks",
          "Affected Blocks",
          "Updated Points",
          "Encoding Time(ns)",
          "Update Time Fixed(ns)",
          "Compressed Size(Fixed)",
          "Compression Ratio(Fixed)",
          "Update Time Recompute(ns)",
          "Compressed Size(Recompute)",
          "Compression Ratio(Recompute)",
          "Need Recompute Beta",
          "Beta Changed Blocks",
          "Memcpy Blocks(Fixed)",
          "Reencoded Blocks(Fixed)",
          "Memcpy Blocks(Recompute)",
          "Reencoded Blocks(Recompute)",
          "Points",
          "Compressed Size(Original)",
          "Compression Ratio(Original)"
        });

    File directory = new File(inputParentDir);
    File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
    if (csvFiles == null) {
      writer.close();
      return;
    }

    for (File file : csvFiles) {
      String datasetName = SubcolumnAddDictPruneNewUpdateTestUtil.extractFileName(file.toString());
      if (datasetName.equals("POI-lon") || datasetName.equals("POI-lat")) {
        continue;
      }
      System.out.println(datasetName);

      SubcolumnAddDictPruneNewUpdateTestUtil.DataStats stats =
          SubcolumnAddDictPruneNewUpdateTestUtil.loadDatasetAsIntArray(file);
      int[] origin = stats.values;
      if (origin.length == 0) {
        continue;
      }

      int numBlocks = origin.length / blockSize;
      int remainder = origin.length % blockSize;
      int totalBlocks = numBlocks + (remainder > 0 ? 1 : 0);

      int[] metaStart = new int[totalBlocks];
      int[] metaEnd = new int[totalBlocks];
      int[] metaRowCount = new int[totalBlocks];
      int[] metaBeta = new int[totalBlocks];
      boolean[] metaSubcolumn = new boolean[totalBlocks];
      int[][] metaEncodingType = new int[totalBlocks][];

      byte[] encodedOrigin = new byte[Math.max(16, origin.length * 20)];
      int encodedLength = 0;
      long start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        encodedLength = SubcolumnPruneNewTest.Encoder(origin, blockSize, encodedOrigin);
      }
      long end = System.nanoTime();
      long encodeTime = (end - start) / repeatTime;
      encodedLength = SubcolumnPruneNewTest.Encoder(origin, blockSize, encodedOrigin);

      parseBlockMetas(
          encodedOrigin,
          encodedLength,
          blockSize,
          metaStart,
          metaEnd,
          metaRowCount,
          metaBeta,
          metaSubcolumn,
          metaEncodingType);

      int[] fixedStats = new int[4];
      int[] recomputeStats = new int[4];

      for (int ratio : UPDATE_RATIOS) {
        int[] updated = buildUpdatedValues(origin, ratio, blockSize);
        boolean[] affected = markAffectedBlocks(origin, updated, blockSize, totalBlocks);
        int updatedPoints = countUpdatedPoints(origin, updated);
        int affectedBlockCount = countTrue(affected);

        byte[] encodedUpdatedFixed = new byte[Math.max(16, origin.length * 20)];
        byte[] encodedUpdatedRecompute = new byte[Math.max(16, origin.length * 20)];

        long updateStart = System.nanoTime();
        for (int repeat = 0; repeat < repeatTime; repeat++) {
          mergeEncodedByBlockFixed(
              updated,
              blockSize,
              numBlocks,
              remainder,
              encodedOrigin,
              metaStart,
              metaEnd,
              metaRowCount,
              metaBeta,
              metaSubcolumn,
              metaEncodingType,
              affected,
              encodedUpdatedFixed,
              fixedStats);
        }
        long updateEnd = System.nanoTime();
        long updateTimeFixed = (updateEnd - updateStart) / repeatTime;

        updateStart = System.nanoTime();
        for (int repeat = 0; repeat < repeatTime; repeat++) {
          mergeEncodedByBlockRecompute(
              updated,
              blockSize,
              numBlocks,
              remainder,
              encodedOrigin,
              metaStart,
              metaEnd,
              metaRowCount,
              metaBeta,
              metaSubcolumn,
              affected,
              encodedUpdatedRecompute,
              recomputeStats);
        }
        updateEnd = System.nanoTime();
        long updateTimeRecompute = (updateEnd - updateStart) / repeatTime;

        int updatedLengthFixed = fixedStats[STATS_UPDATED_LENGTH];
        int updatedLengthRecompute = recomputeStats[STATS_UPDATED_LENGTH];
        int betaChangedBlocks = recomputeStats[STATS_BETA_CHANGED];
        int fixedReencodedBlocks = fixedStats[STATS_REENCODED_BLOCKS];
        int fixedMemcpyBlocks = fixedStats[STATS_MEMCPY_BLOCKS];
        int recomputeReencodedBlocks = recomputeStats[STATS_REENCODED_BLOCKS];
        int recomputeMemcpyBlocks = recomputeStats[STATS_MEMCPY_BLOCKS];

        double originalRatio = encodedLength / (double) (origin.length * Long.BYTES);
        double fixedRatio = updatedLengthFixed / (double) (origin.length * Long.BYTES);
        double recomputeRatio = updatedLengthRecompute / (double) (origin.length * Long.BYTES);
        writer.writeRecord(
            new String[] {
              datasetName,
              "SubcolumnAddDictPruneNew",
              String.valueOf(ratio),
              String.valueOf(totalBlocks),
              String.valueOf(affectedBlockCount),
              String.valueOf(updatedPoints),
              String.valueOf(encodeTime),
              String.valueOf(updateTimeFixed),
              String.valueOf(updatedLengthFixed),
              String.valueOf(fixedRatio),
              String.valueOf(updateTimeRecompute),
              String.valueOf(updatedLengthRecompute),
              String.valueOf(recomputeRatio),
              String.valueOf(betaChangedBlocks > 0),
              String.valueOf(betaChangedBlocks),
              String.valueOf(fixedMemcpyBlocks),
              String.valueOf(fixedReencodedBlocks),
              String.valueOf(recomputeMemcpyBlocks),
              String.valueOf(recomputeReencodedBlocks),
              String.valueOf(origin.length),
              String.valueOf(encodedLength),
              String.valueOf(originalRatio)
            });
      }
    }
    writer.close();
  }

  private static int[] buildUpdatedValues(int[] origin, int ratio, int blockSize) {
    int[] updated = Arrays.copyOf(origin, origin.length);
    if (origin.length == 0) {
      return updated;
    }

    int targetUpdateCount = (int) Math.ceil(origin.length * (ratio / 100.0));
    if (targetUpdateCount <= 0) {
      return updated;
    }

    if (CLUSTER_UPDATES_IN_CONTIGUOUS_WINDOW) {
      int window = Math.min(targetUpdateCount, origin.length);
      int start = Math.max(0, (origin.length - window) / 2);
      for (int i = 0; i < window; i++) {
        int index = start + i;
        int prevValue = (index > 0) ? origin[index - 1] : origin[index];
        int nextValue = (index < origin.length - 1) ? origin[index + 1] : origin[index];
        updated[index] = (prevValue + nextValue) / 2;
      }
      return updated;
    }

    int blockCount = (origin.length + blockSize - 1) / blockSize;
    int[] blockMin = new int[blockCount];
    int[] blockMax = new int[blockCount];
    Arrays.fill(blockMin, Integer.MAX_VALUE);
    Arrays.fill(blockMax, Integer.MIN_VALUE);
    for (int i = 0; i < origin.length; i++) {
      int blockIndex = i / blockSize;
      int value = origin[i];
      if (value < blockMin[blockIndex]) {
        blockMin[blockIndex] = value;
      }
      if (value > blockMax[blockIndex]) {
        blockMax[blockIndex] = value;
      }
    }

    // Random random = new Random();
    boolean[] used = new boolean[origin.length];
    for (int i = 0; i < targetUpdateCount; i++) {
      int index = (int) (((long) i * origin.length) / targetUpdateCount);
      if (index >= origin.length) {
        index = origin.length - 1;
      }
      while (used[index]) {
        index++;
        if (index >= origin.length) {
          index = 0;
        }
      }
      used[index] = true;

      int prevValue = (index > 0) ? origin[index - 1] : origin[index];
      int nextValue = (index < origin.length - 1) ? origin[index + 1] : origin[index];
      updated[index] = (prevValue + nextValue) / 2;
      // updated[index] = nextValue;

      /*
      int blockIndex = index / blockSize;
      int minValue = blockMin[blockIndex];
      int maxValue = blockMax[blockIndex];
      if (minValue >= maxValue) {
        updated[index] = maxValue;
        continue;
      }

      int oldValue = origin[index];
      int candidate = oldValue;
      for (int attempt = 0; attempt < 6 && candidate == oldValue; attempt++) {
        long span = (long) maxValue - (long) minValue + 1L;
        long offset = nextLongBounded(random, span);
        candidate = (int) ((long) minValue + offset);
      }
      if (candidate == oldValue) {
        candidate = (oldValue == maxValue) ? oldValue - 1 : oldValue + 1;
      }
      updated[index] = candidate;
      */
    }
    return updated;
  }

  /*
  private static long nextLongBounded(Random random, long bound) {
    if (bound <= 0L) {
      throw new IllegalArgumentException("bound must be positive");
    }
    long m = bound - 1L;
    if ((bound & m) == 0L) {
      return random.nextLong() & m;
    }
    long u = random.nextLong() >>> 1;
    while (u + m - (u % bound) < 0L) {
      u = random.nextLong() >>> 1;
    }
    return u % bound;
  }
  */

  private static boolean[] markAffectedBlocks(
      int[] origin, int[] updated, int blockSize, int blockCount) {
    boolean[] affected = new boolean[blockCount];
    for (int i = 0; i < origin.length; i++) {
      if (origin[i] != updated[i]) {
        int blockIndex = i / blockSize;
        if (blockIndex >= 0 && blockIndex < affected.length) {
          affected[blockIndex] = true;
        }
      }
    }
    return affected;
  }

  private static int countTrue(boolean[] flags) {
    int c = 0;
    for (boolean f : flags) {
      if (f) {
        c++;
      }
    }
    return c;
  }

  private static int countUpdatedPoints(int[] origin, int[] updated) {
    int changed = 0;
    for (int i = 0; i < origin.length; i++) {
      if (origin[i] != updated[i]) {
        changed++;
      }
    }
    return changed;
  }

  /** 固定策略：复用原块 beta 与原编码类型模板，不重算最优 beta/编码类型。 */
  private static void mergeEncodedByBlockFixed(
      int[] updated,
      int blockSize,
      int numBlocks,
      int remainder,
      byte[] encodedOrigin,
      int[] metaStart,
      int[] metaEnd,
      int[] metaRowCount,
      int[] metaBeta,
      boolean[] metaSubcolumn,
      int[][] metaEncodingType,
      boolean[] affected,
      byte[] outputBuffer,
      int[] mergeStats) {
    SubcolumnPruneNewTest.int2Bytes(updated.length, 0, outputBuffer);
    SubcolumnPruneNewTest.int2Bytes(blockSize, 4, outputBuffer);

    int encodePos = 8;
    int reencodedBlocks = 0;
    int memcpyBlocks = 0;
    int totalBlocks = metaStart.length;

    for (int i = 0; i < totalBlocks; i++) {
      if (!affected[i]) {
        int len = metaEnd[i] - metaStart[i];
        System.arraycopy(encodedOrigin, metaStart[i], outputBuffer, encodePos, len);
        encodePos += len;
        memcpyBlocks++;
        continue;
      }

      reencodedBlocks++;
      if (!metaSubcolumn[i]) {
        int encIdx = encoderBlockIndex(i, numBlocks, remainder);
        int base = encIdx * blockSize;
        for (int k = 0; k < metaRowCount[i]; k++) {
          SubcolumnPruneNewTest.int2Bytes(updated[base + k], encodePos, outputBuffer);
          encodePos += 4;
        }
        continue;
      }

      int encIdx = encoderBlockIndex(i, numBlocks, remainder);
      int fixedBeta = metaBeta[i] > 0 ? metaBeta[i] : 2;
      encodePos =
          blockEncodeWithFixedScheme(
              updated,
              encIdx,
              blockSize,
              metaRowCount[i],
              encodePos,
              outputBuffer,
              fixedBeta,
              metaEncodingType[i]);
    }

    mergeStats[STATS_UPDATED_LENGTH] = encodePos;
    mergeStats[STATS_BETA_CHANGED] = 0;
    mergeStats[STATS_REENCODED_BLOCKS] = reencodedBlocks;
    mergeStats[STATS_MEMCPY_BLOCKS] = memcpyBlocks;
  }

  /** 自适应策略：重算每个更新块的最优 beta/编码类型。 */
  private static void mergeEncodedByBlockRecompute(
      int[] updated,
      int blockSize,
      int numBlocks,
      int remainder,
      byte[] encodedOrigin,
      int[] metaStart,
      int[] metaEnd,
      int[] metaRowCount,
      int[] metaBeta,
      boolean[] metaSubcolumn,
      boolean[] affected,
      byte[] outputBuffer,
      int[] mergeStats) {
    SubcolumnPruneNewTest.int2Bytes(updated.length, 0, outputBuffer);
    SubcolumnPruneNewTest.int2Bytes(blockSize, 4, outputBuffer);

    int encodePos = 8;
    int betaChangedBlocks = 0;
    int reencodedBlocks = 0;
    int memcpyBlocks = 0;
    int totalBlocks = metaStart.length;

    for (int i = 0; i < totalBlocks; i++) {
      if (!affected[i]) {
        int len = metaEnd[i] - metaStart[i];
        System.arraycopy(encodedOrigin, metaStart[i], outputBuffer, encodePos, len);
        encodePos += len;
        memcpyBlocks++;
        continue;
      }

      reencodedBlocks++;
      if (!metaSubcolumn[i]) {
        int encIdx = encoderBlockIndex(i, numBlocks, remainder);
        int base = encIdx * blockSize;
        for (int k = 0; k < metaRowCount[i]; k++) {
          SubcolumnPruneNewTest.int2Bytes(updated[base + k], encodePos, outputBuffer);
          encodePos += 4;
        }
        continue;
      }

      int[] beta = new int[] {2};
      int encIdx = encoderBlockIndex(i, numBlocks, remainder);
      int newPos =
          SubcolumnPruneNewTest.BlockEncoder(
              updated, encIdx, blockSize, metaRowCount[i], encodePos, outputBuffer, beta);
      if (metaBeta[i] > 0 && beta[0] != metaBeta[i]) {
        betaChangedBlocks++;
      }
      encodePos = newPos;
    }

    mergeStats[STATS_UPDATED_LENGTH] = encodePos;
    mergeStats[STATS_BETA_CHANGED] = betaChangedBlocks;
    mergeStats[STATS_REENCODED_BLOCKS] = reencodedBlocks;
    mergeStats[STATS_MEMCPY_BLOCKS] = memcpyBlocks;
  }

  private static int blockEncodeWithFixedScheme(
      int[] data,
      int blockIndex,
      int blockSize,
      int rowCount,
      int encodePos,
      byte[] encodedResult,
      int fixedBeta,
      int[] originalEncodingType) {
    int[] minDelta = new int[1];
    int[] dataDelta =
        SubcolumnPruneNewTest.getAbsDeltaTsBlock(data, blockIndex, blockSize, rowCount, minDelta);

    SubcolumnPruneNewTest.int2Bytes(minDelta[0], encodePos, encodedResult);
    encodePos += 4;

    int maxValue = 0;
    for (int j = 0; j < rowCount; j++) {
      if (dataDelta[j] > maxValue) {
        maxValue = dataDelta[j];
      }
    }

    int m = SubcolumnPruneNewTest.bitWidth(maxValue);
    int betaValue = fixedBeta > 0 ? fixedBeta : 2;
    int[] beta = new int[] {betaValue};
    int[] encodingType = adaptEncodingType(originalEncodingType, m, betaValue);
    return SubcolumnPruneNewTest.SubcolumnEncoder(
        dataDelta, encodePos, encodedResult, beta, blockSize, encodingType);
  }

  private static int[] adaptEncodingType(int[] originalEncodingType, int m, int betaValue) {
    if (m == 0) {
      return new int[0];
    }
    int l = (m + betaValue - 1) / betaValue;
    int[] result = new int[l];
    if (originalEncodingType == null) {
      return result;
    }
    int copyLen = Math.min(l, originalEncodingType.length);
    for (int i = 0; i < copyLen; i++) {
      int type = originalEncodingType[i];
      result[i] = (type >= 0 && type <= 2) ? type : 0;
    }
    return result;
  }

  private static int encoderBlockIndex(int blockSlot, int numBlocks, int remainder) {
    if (remainder > 0 && blockSlot == numBlocks) {
      return numBlocks;
    }
    return blockSlot;
  }

  private static void parseBlockMetas(
      byte[] encoded,
      int encodedLength,
      int expectedBlockSize,
      int[] metaStart,
      int[] metaEnd,
      int[] metaRowCount,
      int[] metaBeta,
      boolean[] metaSubcolumn,
      int[][] metaEncodingType) {
    int dataLength = SubcolumnPruneNewTest.bytes2Integer(encoded, 0, 4);
    int parsedBlockSize = SubcolumnPruneNewTest.bytes2Integer(encoded, 4, 4);
    if (parsedBlockSize != expectedBlockSize) {
      throw new IllegalStateException("blockSize mismatch header vs test.");
    }
    int numBlocks = dataLength / parsedBlockSize;
    int remainder = dataLength % parsedBlockSize;
    int pos = 8;
    int tb = 0;

    for (int blockIndex = 0; blockIndex < numBlocks; blockIndex++) {
      metaStart[tb] = pos;
      metaRowCount[tb] = parsedBlockSize;
      int[] betaHolder = new int[1];
      pos =
          parseSubcolumnBlock(
              encoded,
              pos,
              parsedBlockSize,
              parsedBlockSize,
              betaHolder,
              metaEncodingType,
              tb);
      metaBeta[tb] = betaHolder[0];
      metaSubcolumn[tb] = true;
      metaEnd[tb] = pos;
      tb++;
    }

    if (remainder > 0) {
      metaStart[tb] = pos;
      metaRowCount[tb] = remainder;
      if (remainder <= 3) {
        metaSubcolumn[tb] = false;
        metaBeta[tb] = -1;
        metaEncodingType[tb] = new int[0];
        pos += remainder * 4;
      } else {
        int[] betaHolder = new int[1];
        pos =
            parseSubcolumnBlock(
                encoded, pos, parsedBlockSize, remainder, betaHolder, metaEncodingType, tb);
        metaBeta[tb] = betaHolder[0];
        metaSubcolumn[tb] = true;
      }
      metaEnd[tb] = pos;
      tb++;
    }

    if (pos > encodedLength) {
      throw new IllegalStateException("Encoded stream parsing overflow.");
    }
  }

  private static int parseSubcolumnBlock(
      byte[] encoded,
      int encodePos,
      int blockSize,
      int rowCount,
      int[] betaOut,
      int[][] metaEncodingType,
      int blockSlot) {
    encodePos += 4;
    int m = SubcolumnPruneNewTest.bytes2Integer(encoded, encodePos, 1);
    encodePos += 1;
    if (m == 0) {
      betaOut[0] = 1;
      metaEncodingType[blockSlot] = new int[0];
      return encodePos;
    }

    int beta = SubcolumnPruneNewTest.bytes2Integer(encoded, encodePos, 1);
    betaOut[0] = beta;
    encodePos += 1;
    int l = (m + beta - 1) / beta;

    int[] bitWidthList = new int[l];
    encodePos = SubcolumnPruneNewTest.decodeBitPacking(encoded, encodePos, 8, l, bitWidthList);

    int[] encodingType = new int[l];
    encodePos = SubcolumnPruneNewTest.decodeBitPacking(encoded, encodePos, 2, l, encodingType);
    metaEncodingType[blockSlot] = Arrays.copyOf(encodingType, l);

    int bw = SubcolumnPruneNewTest.bitWidth(blockSize);
    int scanPos = encodePos;
    for (int i = 0; i < l; i++) {
      int type = encodingType[i];
      int bitWidth = bitWidthList[i];
      if (type == 0) {
        long bitPos = ((long) scanPos) * 8L + (long) bitWidth * rowCount;
        scanPos = (int) ((bitPos + 7L) / 8L);
      } else if (type == 1) {
        int runCount = ((encoded[scanPos] & 0xFF) << 8) | (encoded[scanPos + 1] & 0xFF);
        scanPos += 2;
        long bitPos = ((long) scanPos) * 8L + (long) runCount * bw;
        scanPos = (int) ((bitPos + 7L) / 8L);
        bitPos = ((long) scanPos) * 8L + (long) runCount * bitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
      } else {
        int cardinality = ((encoded[scanPos] & 0xFF) << 8) | (encoded[scanPos + 1] & 0xFF);
        scanPos += 2;
        int dictBitWidth = SubcolumnPruneNewTest.bitWidth(cardinality);
        long bitPos = ((long) scanPos) * 8L + (long) cardinality * bitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
        bitPos = ((long) scanPos) * 8L + (long) rowCount * dictBitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
      }
    }
    return scanPos;
  }
}
