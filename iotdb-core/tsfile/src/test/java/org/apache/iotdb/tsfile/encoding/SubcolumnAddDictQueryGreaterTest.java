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
import java.util.HashMap;

public class SubcolumnAddDictQueryGreaterTest {

  public static void Query(byte[] encodedResult, int lowerBound) {
    int encodePos = 0;

    int dataLength = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;

    int blockSize = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;

    int numBlocks = dataLength / blockSize;
    int[] result = new int[dataLength];
    int[] resultLength = new int[1];

    for (int i = 0; i < numBlocks; i++) {
      encodePos =
          BlockQueryIndex(
              encodedResult, i, blockSize, blockSize, encodePos, lowerBound, result, resultLength);
    }

    int remainder = dataLength % blockSize;
    if (remainder <= 3) {
      int base = numBlocks * blockSize;
      for (int i = 0; i < remainder; i++) {
        int value = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
        if (value > lowerBound) {
          result[resultLength[0]++] = base + i;
        }
        encodePos += 4;
      }
    } else {
      encodePos =
          BlockQueryIndex(
              encodedResult,
              numBlocks,
              blockSize,
              remainder,
              encodePos,
              lowerBound,
              result,
              resultLength);
    }
  }

  public static int BlockQueryIndex(
      byte[] encodedResult,
      int blockIndex,
      int blockSize,
      int remainder,
      int encodePos,
      int lowerBound,
      int[] result,
      int[] resultLength) {
    int minDelta = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;
    int adjustedLower = lowerBound - minDelta;
    int base = blockIndex * blockSize;

    int m = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 1);
    encodePos += 1;
    if (m == 0) {
      if (adjustedLower < 0) {
        for (int i = 0; i < remainder; i++) {
          result[resultLength[0]++] = base + i;
        }
      }
      return encodePos;
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

    // Payload is encoded in i=0..l-1 order. First pass records offsets and computes end position.
    for (int i = 0; i < l; i++) {
      segmentPos[i] = scanPos;
      int type = encodingType[i];
      int currentBitWidth = bitWidthList[i];

      if (type == 0) {
        long bitPos = ((long) scanPos) * 8L + (long) currentBitWidth * remainder;
        scanPos = (int) ((bitPos + 7L) / 8L);
      } else if (type == 1) {
        int runCount =
            ((encodedResult[scanPos] & 0xFF) << 8) | (encodedResult[scanPos + 1] & 0xFF);
        runCountList[i] = runCount;
        scanPos += 2;

        long bitPos = ((long) scanPos) * 8L + (long) runCount * bw;
        scanPos = (int) ((bitPos + 7L) / 8L);
        bitPos = ((long) scanPos) * 8L + (long) runCount * currentBitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
      } else {
        int cardinality =
            ((encodedResult[scanPos] & 0xFF) << 8) | (encodedResult[scanPos + 1] & 0xFF);
        cardinalityList[i] = cardinality;
        scanPos += 2;

        int dictBitWidth = SubcolumnPruneNewTest.bitWidth(cardinality);
        long bitPos = ((long) scanPos) * 8L + (long) cardinality * currentBitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
        bitPos = ((long) scanPos) * 8L + (long) remainder * dictBitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
      }
    }

    if (adjustedLower < 0) {
      for (int i = 0; i < remainder; i++) {
        result[resultLength[0]++] = base + i;
      }
      return scanPos;
    }

    int[] candidateIndices = new int[remainder];
    int candidateLength = remainder;
    for (int i = 0; i < remainder; i++) {
      candidateIndices[i] = i;
    }

    // Query compares from high to low subcolumn.
    for (int i = l - 1; i >= 0; i--) {
      int type = encodingType[i];
      int currentBitWidth = bitWidthList[i];
      int boundPart = (adjustedLower >> (i * beta)) & ((1 << beta) - 1);

      if (type == 0) {
        int pos = segmentPos[i];
        long bitStart = ((long) pos) * 8L;

        int newLength = 0;
        for (int j = 0; j < candidateLength; j++) {
          int index = candidateIndices[j];
          int current =
              SubcolumnPruneNewTest.bytesToInt(
                  encodedResult, (int) (bitStart + (long) index * currentBitWidth), currentBitWidth);
          if (current > boundPart) {
            result[resultLength[0]++] = base + index;
          } else if (current == boundPart) {
            candidateIndices[newLength++] = index;
          }
        }
        candidateLength = newLength;
      } else if (type == 1) {
        int pos = segmentPos[i];
        int runCount = runCountList[i];
        pos += 2;

        int[] runEnd = new int[runCount];
        int[] rleValues = new int[runCount];
        pos = SubcolumnPruneNewTest.decodeBitPacking(encodedResult, pos, bw, runCount, runEnd);
        SubcolumnPruneNewTest.decodeBitPacking(
            encodedResult, pos, currentBitWidth, runCount, rleValues);

        int newLength = 0;
        int runIdx = 0;
        for (int j = 0; j < candidateLength; j++) {
          int index = candidateIndices[j];
          while (runIdx < runCount && runEnd[runIdx] <= index) {
            runIdx++;
          }
          if (runIdx >= runCount) {
            break;
          }
          int current = rleValues[runIdx];
          if (current > boundPart) {
            result[resultLength[0]++] = base + index;
          } else if (current == boundPart) {
            candidateIndices[newLength++] = index;
          }
        }
        candidateLength = newLength;
      } else {
        int pos = segmentPos[i];
        int cardinality = cardinalityList[i];
        pos += 2;
        int dictBitWidth = SubcolumnPruneNewTest.bitWidth(cardinality);

        int[] dictKeyList = new int[cardinality];
        int[] dictIndexes = new int[remainder];
        pos =
            SubcolumnPruneNewTest.decodeBitPacking(
                encodedResult, pos, currentBitWidth, cardinality, dictKeyList);
        SubcolumnPruneNewTest.decodeBitPacking(
            encodedResult, pos, dictBitWidth, remainder, dictIndexes);

        int newLength = 0;
        for (int j = 0; j < candidateLength; j++) {
          int index = candidateIndices[j];
          int current = dictKeyList[dictIndexes[index]];
          if (current > boundPart) {
            result[resultLength[0]++] = base + index;
          } else if (current == boundPart) {
            candidateIndices[newLength++] = index;
          }
        }
        candidateLength = newLength;
      }
    }

    return scanPos;
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
    String parentDir = "D://github/xjz17/subcolumn/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/";
    String outputPath = outputParentDir + "subcolumn_adddict_prunenew_query_greater.csv";

    int blockSize = 512;
    int repeatTime = 100;
    System.out.println("Output: " + outputPath);
    System.out.println("Block size: " + blockSize);
    System.out.println("Repeat time: " + repeatTime);

    HashMap<String, Integer> queryRange = new HashMap<>();
    queryRange.put("Bird-migration", 2500000);
    queryRange.put("Bitcoin-price", 160000000);
    queryRange.put("City-temp", 480);
    queryRange.put("Dewpoint-temp", 9500);
    queryRange.put("IR-bio-temp", -300);
    queryRange.put("PM10-dust", 1000);
    queryRange.put("Stocks-DE", 40000);
    queryRange.put("Stocks-UK", 20000);
    queryRange.put("Stocks-USA", 5000);
    queryRange.put("Wind-Speed", 50);
    queryRange.put("Wine-Tasting", 0);
    queryRange.put("Arade4", 10000000);
    queryRange.put("EPM-Education", 200);
    queryRange.put("POI-lat", 0);
    queryRange.put("Gov10", 100000);

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

      byte[] encodedResult = new byte[dataArr.length * 8];
      int length = 0;

      long encodeTime;
      long queryTime;

      long start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        length = SubcolumnPruneNewTest.Encoder(dataArr, blockSize, encodedResult);
      }
      long end = System.nanoTime();
      encodeTime = (end - start) / repeatTime;

      System.out.println("Query");
      start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        Query(encodedResult, queryRange.get(datasetName));
      }
      end = System.nanoTime();
      queryTime = (end - start) / repeatTime;

      double compressionRatio = length / (double) (data.size() * Long.BYTES);
      writer.writeRecord(
          new String[] {
            datasetName,
            "SubcolumnAddDictPruneNew",
            String.valueOf(encodeTime),
            String.valueOf(queryTime),
            String.valueOf(data.size()),
            String.valueOf(length),
            String.valueOf(compressionRatio)
          });
      System.out.println("compressionRatio: " + compressionRatio);
    }

    writer.close();
  }
}
