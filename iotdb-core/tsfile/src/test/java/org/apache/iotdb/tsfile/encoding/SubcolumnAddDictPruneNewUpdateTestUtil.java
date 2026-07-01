package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

class SubcolumnAddDictPruneNewUpdateTestUtil {

  static class TailInfo {
    int tailStartPos;
    int numBlocks;
    int remainder;
  }

  static class DataStats {
    int[] values;
    int minValue;
    int maxValue;
  }

  private SubcolumnAddDictPruneNewUpdateTestUtil() {}

  static int getDecimalPrecision(String str) {
    int decimalIndex = str.indexOf('.');
    if (decimalIndex == -1) {
      return 0;
    }
    return str.length() - decimalIndex - 1;
  }

  static String extractFileName(String path) {
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

  static DataStats loadDatasetAsIntArray(File file) throws IOException {
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
    int maxMul = (int) Math.pow(10, maxDecimal);

    DataStats stats = new DataStats();
    stats.values = new int[data.size()];
    stats.minValue = Integer.MAX_VALUE;
    stats.maxValue = Integer.MIN_VALUE;
    for (int i = 0; i < data.size(); i++) {
      stats.values[i] = (int) (data.get(i) * maxMul);
      if (stats.values[i] < stats.minValue) {
        stats.minValue = stats.values[i];
      }
      if (stats.values[i] > stats.maxValue) {
        stats.maxValue = stats.values[i];
      }
    }
    if (data.isEmpty()) {
      stats.minValue = 0;
      stats.maxValue = 0;
    }
    return stats;
  }

  private static int skipBlock(byte[] encodedResult, int encodePos, int blockSize, int rowCount) {
    encodePos += 4;
    int m = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 1);
    encodePos += 1;
    if (m == 0) {
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
    int scanPos = encodePos;
    for (int i = 0; i < l; i++) {
      int type = encodingType[i];
      int bitWidth = bitWidthList[i];
      if (type == 0) {
        long bitPos = ((long) scanPos) * 8L + (long) bitWidth * rowCount;
        scanPos = (int) ((bitPos + 7L) / 8L);
      } else if (type == 1) {
        int runCount = ((encodedResult[scanPos] & 0xFF) << 8) | (encodedResult[scanPos + 1] & 0xFF);
        scanPos += 2;
        long bitPos = ((long) scanPos) * 8L + (long) runCount * bw;
        scanPos = (int) ((bitPos + 7L) / 8L);
        bitPos = ((long) scanPos) * 8L + (long) runCount * bitWidth;
        scanPos = (int) ((bitPos + 7L) / 8L);
      } else {
        int cardinality =
            ((encodedResult[scanPos] & 0xFF) << 8) | (encodedResult[scanPos + 1] & 0xFF);
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

  static TailInfo locateTailInfo(byte[] encodedResult, int encodedLength) {
    TailInfo info = new TailInfo();
    int encodePos = 0;
    int dataLength = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;
    int blockSize = SubcolumnPruneNewTest.bytes2Integer(encodedResult, encodePos, 4);
    encodePos += 4;

    int numBlocks = dataLength / blockSize;
    int remainder = dataLength % blockSize;
    info.numBlocks = numBlocks;
    info.remainder = remainder;

    for (int i = 0; i < numBlocks; i++) {
      encodePos = skipBlock(encodedResult, encodePos, blockSize, blockSize);
    }
    info.tailStartPos = (remainder > 0) ? encodePos : encodedLength;
    return info;
  }
}
