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

public class SubcolumnQuerySortTest {

  private static final int[] BLOCK_SIZES = {32, 64, 128, 256, 512, 1024, 2048, 4096, 8192};

  private static class BlockSortResult {
    int nextEncodePos;
    int[] sortedIndices;
  }

  private static class Segment {
    int start;
    int end;

    Segment(int start, int end) {
      this.start = start;
      this.end = end;
    }
  }

  // Sort indices inside one block only.
  public static int[] QuerySortSingleBlock(byte[] encodedResult, int targetBlockId) {
    int encodePos = 0;
    int dataLength = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;
    int blockSize = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;

    int numBlocks = dataLength / blockSize;
    int remainder = dataLength % blockSize;
    int totalBlocks = numBlocks + (remainder > 0 ? 1 : 0);
    if (targetBlockId < 0 || targetBlockId >= totalBlocks) {
      return new int[0];
    }

    for (int i = 0; i < numBlocks; i++) {
      BlockSortResult result = blockSort(encodedResult, encodePos, i, blockSize, blockSize);
      encodePos = result.nextEncodePos;
      if (i == targetBlockId) {
        return result.sortedIndices;
      }
    }

    if (remainder > 0) {
      int blockId = numBlocks;
      int base = blockId * blockSize;
      if (remainder <= 3) {
        int[] indices = new int[remainder];
        int[] values = new int[remainder];
        for (int i = 0; i < remainder; i++) {
          values[i] = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
          indices[i] = base + i;
          encodePos += 4;
        }
        sortSmallByValue(indices, values);
        if (blockId == targetBlockId) {
          return indices;
        }
      } else {
        BlockSortResult result = blockSort(encodedResult, encodePos, blockId, blockSize, remainder);
        encodePos = result.nextEncodePos;
        if (blockId == targetBlockId) {
          return result.sortedIndices;
        }
      }
    }

    return new int[0];
  }

  // Baseline: fully decode all values, then sort one block by value and index.
  public static int[] QuerySortByDecodeSingleBlock(byte[] encodedResult, int targetBlockId) {
    int[] decoded = SubcolumnPruneNewTest.Decoder(encodedResult);
    int dataLength = decoded.length;
    int blockSize = SubcolumnPruneNewTest.bytes2Integer(encodedResult, 4, 4);
    int numBlocks = dataLength / blockSize;
    int remainder = dataLength % blockSize;
    int totalBlocks = numBlocks + (remainder > 0 ? 1 : 0);
    if (targetBlockId < 0 || targetBlockId >= totalBlocks) {
      return new int[0];
    }

    int count = targetBlockId < numBlocks ? blockSize : remainder;
    int start = targetBlockId * blockSize;
    int[] index = new int[count];
    for (int i = 0; i < count; i++) {
      index[i] = start + i;
    }
    sortIndicesByValue(index, decoded);
    return index;
  }

  private static BlockSortResult blockSort(
      byte[] encodedResult, int encodePos, int blockId, int blockSize, int rowCount) {
    BlockSortResult result = new BlockSortResult();

    SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;
    int m = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 1);
    encodePos += 1;

    int[] order = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      order[i] = i;
    }
    int[] values = new int[rowCount];

    if (m == 0) {
      int[] absIndex = new int[rowCount];
      int base = blockId * blockSize;
      for (int i = 0; i < rowCount; i++) {
        absIndex[i] = base + i;
      }
      result.nextEncodePos = encodePos;
      result.sortedIndices = absIndex;
      return result;
    }

    int beta = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 1);
    encodePos += 1;
    int l = (m + beta - 1) / beta;

    int[] bitWidthList = new int[l];
    encodePos =
        SubcolumnPruneNewTest.decodeBitPacking(encodedResult, encodePos, 8, l, bitWidthList);
    int[] encodingType = new int[l];
    encodePos =
        SubcolumnPruneNewTest.decodeBitPacking(encodedResult, encodePos, 2, l, encodingType);

    int bw = SubcolumnPruneNewTest.bitWidth(blockSize);
    int[] segmentPos = new int[l];
    int[] runCountList = new int[l];
    int[] cardinalityList = new int[l];
    int scanPos = encodePos;

    for (int i = 0; i < l; i++) {
      segmentPos[i] = scanPos;
      int type = encodingType[i];
      int currentBitWidth = bitWidthList[i];
      if (type == 0) {
        long bitPos = ((long) scanPos) * 8L + (long) currentBitWidth * rowCount;
        scanPos = (int) ((bitPos + 7L) / 8L);
      } else if (type == 1) {
        int runCount = ((encodedResult[scanPos] & 0xFF) << 8) | (encodedResult[scanPos + 1] & 0xFF);
        runCountList[i] = runCount;
        scanPos += 2;
        long bitPos = ((long) scanPos) * 8L + (long) runCount * bw;
        scanPos = (int) ((bitPos + 7L) / 8L);
        bitPos = ((long) scanPos) * 8L + (long) runCount * currentBitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
      } else {
        int cardinality = ((encodedResult[scanPos] & 0xFF) << 8) | (encodedResult[scanPos + 1] & 0xFF);
        cardinalityList[i] = cardinality;
        scanPos += 2;
        int dictBitWidth = SubcolumnPruneNewTest.bitWidth(cardinality);
        long bitPos = ((long) scanPos) * 8L + (long) cardinality * currentBitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
        bitPos = ((long) scanPos) * 8L + (long) rowCount * dictBitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
      }
    }

    ArrayList<Segment> segments = new ArrayList<>();
    segments.add(new Segment(0, rowCount));

    for (int i = l - 1; i >= 0; i--) {
      int[] digitValues =
          decodeSubcolumnValues(
              encodedResult,
              segmentPos[i],
              encodingType[i],
              bitWidthList[i],
              rowCount,
              bw,
              runCountList[i],
              cardinalityList[i]);

      int shift = i * beta;
      for (int r = 0; r < rowCount; r++) {
        values[r] |= digitValues[r] << shift;
      }

      int[] nextOrder = new int[rowCount];
      ArrayList<Segment> nextSegments = new ArrayList<>();

      int cursor = 0;
      for (Segment seg : segments) {
        if (seg.end - seg.start <= 1) {
          nextOrder[cursor++] = order[seg.start];
          nextSegments.add(new Segment(cursor - 1, cursor));
          continue;
        }

        // beta<=4 in current encoder tests; bucket size 16 is enough.
        int[] count = new int[16];
        for (int p = seg.start; p < seg.end; p++) {
          count[digitValues[order[p]]]++;
        }
        int[] start = new int[16];
        int running = cursor;
        for (int b = 0; b < 16; b++) {
          start[b] = running;
          running += count[b];
        }
        int[] write = start.clone();
        for (int p = seg.start; p < seg.end; p++) {
          int idx = order[p];
          int bucket = digitValues[idx];
          nextOrder[write[bucket]++] = idx;
        }

        for (int b = 0; b < 16; b++) {
          if (count[b] > 0) {
            int s = start[b];
            int e = s + count[b];
            nextSegments.add(new Segment(s, e));
          }
        }
        cursor = running;
      }

      order = nextOrder;
      segments = nextSegments;
    }

    int base = blockId * blockSize;
    int[] sortedIndices = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      int local = order[i];
      sortedIndices[i] = base + local;
    }

    result.nextEncodePos = scanPos;
    result.sortedIndices = sortedIndices;
    return result;
  }

  private static int[] decodeSubcolumnValues(
      byte[] encodedResult,
      int segmentPos,
      int type,
      int currentBitWidth,
      int rowCount,
      int bw,
      int runCount,
      int cardinality) {
    int[] result = new int[rowCount];

    if (type == 0) {
      long bitStart = ((long) segmentPos) * 8L;
      for (int i = 0; i < rowCount; i++) {
        result[i] =
            SubcolumnPruneNewTest.bytesToInt(
                encodedResult, (int) (bitStart + (long) i * currentBitWidth), currentBitWidth);
      }
      return result;
    }

    int pos = segmentPos + 2;
    if (type == 1) {
      int[] runEnd = new int[runCount];
      int[] rleValues = new int[runCount];
      pos = SubcolumnPruneNewTest.decodeBitPacking(encodedResult, pos, bw, runCount, runEnd);
      SubcolumnPruneNewTest.decodeBitPacking(
          encodedResult, pos, currentBitWidth, runCount, rleValues);

      int begin = 0;
      for (int i = 0; i < runCount; i++) {
        int end = runEnd[i];
        int value = rleValues[i];
        while (begin < end && begin < rowCount) {
          result[begin++] = value;
        }
      }
      return result;
    }

    int dictBitWidth = SubcolumnPruneNewTest.bitWidth(cardinality);
    int[] dictKeyList = new int[cardinality];
    int[] dictIndexes = new int[rowCount];
    pos =
        SubcolumnPruneNewTest.decodeBitPacking(
            encodedResult, pos, currentBitWidth, cardinality, dictKeyList);
    SubcolumnPruneNewTest.decodeBitPacking(encodedResult, pos, dictBitWidth, rowCount, dictIndexes);

    for (int i = 0; i < rowCount; i++) {
      result[i] = dictKeyList[dictIndexes[i]];
    }
    return result;
  }

  private static void sortSmallByValue(int[] indices, int[] values) {
    for (int i = 0; i < values.length; i++) {
      int minPos = i;
      for (int j = i + 1; j < values.length; j++) {
        if (values[j] < values[minPos]
            || (values[j] == values[minPos] && indices[j] < indices[minPos])) {
          minPos = j;
        }
      }
      if (minPos != i) {
        int tmpV = values[i];
        values[i] = values[minPos];
        values[minPos] = tmpV;
        int tmpI = indices[i];
        indices[i] = indices[minPos];
        indices[minPos] = tmpI;
      }
    }
  }

  private static void sortIndicesByValue(int[] indices, int[] values) {
    for (int i = 0; i < indices.length; i++) {
      int minPos = i;
      for (int j = i + 1; j < indices.length; j++) {
        int vj = values[indices[j]];
        int vm = values[indices[minPos]];
        if (vj < vm || (vj == vm && indices[j] < indices[minPos])) {
          minPos = j;
        }
      }
      if (minPos != i) {
        int tmp = indices[i];
        indices[i] = indices[minPos];
        indices[minPos] = tmp;
      }
    }
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

  private interface BlockSortRunner {
    int[] run(byte[] encodedResult, int blockId);
  }

  private void runSingleBlockSortBenchmark(
      String outputPath, String algorithmName, BlockSortRunner runner) throws IOException {
    String parentDir = "path/to/your/directory/";
    String inputParentDir = parentDir + "dataset/";

    int repeatTime = 100;
    System.out.println("Output: " + outputPath);
    System.out.println("Block sizes: " + java.util.Arrays.toString(BLOCK_SIZES));
    System.out.println("Repeat time: " + repeatTime);

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

      for (int blockSize : BLOCK_SIZES) {
        byte[] encodedResult = new byte[dataArr.length * 8];
        int length = 0;

        long start = System.nanoTime();
        for (int repeat = 0; repeat < repeatTime; repeat++) {
          length = SubcolumnPruneNewTest.Encoder(dataArr, blockSize, encodedResult);
        }
        long end = System.nanoTime();
        long encodeTime = (end - start) / repeatTime;

        int blockId = 0;
        int[] sortedIndex = null;
        start = System.nanoTime();
        for (int repeat = 0; repeat < repeatTime; repeat++) {
          sortedIndex = runner.run(encodedResult, blockId);
        }
        end = System.nanoTime();
        long querySortTime = (end - start) / repeatTime;
        System.out.println(
            "blockSize="
                + blockSize
                + ", blockPoints: "
                + (sortedIndex == null ? 0 : sortedIndex.length));
        double compressionRatio = length / (double) (data.size() * Long.BYTES);
        writer.writeRecord(
            new String[] {
              datasetName,
              algorithmName,
              String.valueOf(blockSize),
              String.valueOf(encodeTime),
              String.valueOf(querySortTime),
              String.valueOf(data.size()),
              String.valueOf(length),
              String.valueOf(compressionRatio)
            });
        System.out.println("compressionRatio: " + compressionRatio);
      }
    }

    writer.close();
  }

  @Test
  public void testOptimizedSort() throws IOException {
    String parentDir = "path/to/your/directory/";
    String outputPath = parentDir + "result/" + "subcolumn_adddict_prunenew_query_sort.csv";
    runSingleBlockSortBenchmark(
        outputPath,
        "SubcolumnAddDictPruneNew",
        SubcolumnQuerySortTest::QuerySortSingleBlock);
  }

  @Test
  public void testDecodeSort() throws IOException {
    String parentDir = "path/to/your/directory/";
    String outputPath = parentDir + "result/" + "subcolumn_adddict_prunenew_query_sort_decode.csv";
    runSingleBlockSortBenchmark(
        outputPath,
        "SubcolumnAddDictPruneNew",
        SubcolumnQuerySortTest::QuerySortByDecodeSingleBlock);
  }
}
