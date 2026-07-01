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

public class TSDIFFSubcolumnPruneNewTest {

  public static int Encoder(int[] data, int blockSize, byte[] encodedResult) {
    return Encoder(data, blockSize, encodedResult, null, null);
  }

  public static int Encoder(
      int[] data, int blockSize, byte[] encodedResult, long[] ts2diffTime, long[] subcolumnTime) {
    int dataLength = data.length;
    int encodePos = 0;

    encodedResult[0] = (byte) (dataLength >> 24);
    encodedResult[1] = (byte) (dataLength >> 16);
    encodedResult[2] = (byte) (dataLength >> 8);
    encodedResult[3] = (byte) dataLength;
    encodePos += 4;

    encodedResult[4] = (byte) (blockSize >> 24);
    encodedResult[5] = (byte) (blockSize >> 16);
    encodedResult[6] = (byte) (blockSize >> 8);
    encodedResult[7] = (byte) blockSize;
    encodePos += 4;

    int numBlocks = dataLength / blockSize;
    int remainder = dataLength % blockSize;
    int[] beta = new int[] {3};

    for (int i = 0; i < numBlocks; i++) {
      encodePos =
          BlockEncoder(
              data, i, blockSize, blockSize, encodePos, encodedResult, beta, ts2diffTime, subcolumnTime);
    }

    if (remainder <= 3) {
      for (int i = 0; i < remainder; i++) {
        int value = data[numBlocks * blockSize + i];
        encodedResult[encodePos] = (byte) (value >> 24);
        encodedResult[encodePos + 1] = (byte) (value >> 16);
        encodedResult[encodePos + 2] = (byte) (value >> 8);
        encodedResult[encodePos + 3] = (byte) value;
        encodePos += 4;
      }
    } else {
      encodePos =
          BlockEncoder(
              data, numBlocks, blockSize, remainder, encodePos, encodedResult, beta, ts2diffTime, subcolumnTime);
    }

    return encodePos;
  }

  public static int[] Decoder(byte[] encodedResult) {
    int encodePos = 0;

    int dataLength =
        ((encodedResult[encodePos] & 0xFF) << 24)
            | ((encodedResult[encodePos + 1] & 0xFF) << 16)
            | ((encodedResult[encodePos + 2] & 0xFF) << 8)
            | (encodedResult[encodePos + 3] & 0xFF);
    encodePos += 4;

    int blockSize =
        ((encodedResult[encodePos] & 0xFF) << 24)
            | ((encodedResult[encodePos + 1] & 0xFF) << 16)
            | ((encodedResult[encodePos + 2] & 0xFF) << 8)
            | (encodedResult[encodePos + 3] & 0xFF);
    encodePos += 4;

    int numBlocks = dataLength / blockSize;
    int[] data = new int[dataLength];

    for (int i = 0; i < numBlocks; i++) {
      encodePos = BlockDecoder(encodedResult, i, blockSize, blockSize, encodePos, data);
    }

    int remainder = dataLength % blockSize;
    if (remainder <= 3) {
      for (int i = 0; i < remainder; i++) {
        data[numBlocks * blockSize + i] =
            ((encodedResult[encodePos] & 0xFF) << 24)
                | ((encodedResult[encodePos + 1] & 0xFF) << 16)
                | ((encodedResult[encodePos + 2] & 0xFF) << 8)
                | (encodedResult[encodePos + 3] & 0xFF);
        encodePos += 4;
      }
    } else {
      encodePos = BlockDecoder(encodedResult, numBlocks, blockSize, remainder, encodePos, data);
    }

    return data;
  }

  public static int[] getAbsDeltaTsBlock(
      int[] tsBlock, int blockIndex, int blockSize, int remaining, int[] minDelta) {
    int[] tsBlockDelta = new int[remaining - 1];
    fillAbsDeltaTsBlock(tsBlock, blockIndex, blockSize, remaining, minDelta, tsBlockDelta);
    return tsBlockDelta;
  }

  private static void fillAbsDeltaTsBlock(
      int[] tsBlock,
      int blockIndex,
      int blockSize,
      int remaining,
      int[] minDelta,
      int[] tsBlockDelta) {
    int valueDeltaMin = Integer.MAX_VALUE;
    int valueDeltaMax = Integer.MIN_VALUE;
    int base = blockIndex * blockSize + 1;
    int end = blockIndex * blockSize + remaining;

    int prev = tsBlock[base - 1];
    minDelta[0] = prev;
    int j = base;
    while (j < end) {
      int cur = tsBlock[j];
      int epsilon = cur - prev;
      tsBlockDelta[j - base] = epsilon;
      if (epsilon < valueDeltaMin) {
        valueDeltaMin = epsilon;
      }
      if (epsilon > valueDeltaMax) {
        valueDeltaMax = epsilon;
      }
      prev = cur;
      j++;
    }

    for (j = 0; j < remaining - 1; j++) {
      tsBlockDelta[j] = tsBlockDelta[j] - valueDeltaMin;
    }

    minDelta[1] = valueDeltaMin;
    minDelta[2] = valueDeltaMax - valueDeltaMin;
  }

  public static int BlockEncoder(
      int[] data,
      int blockIndex,
      int blockSize,
      int remainder,
      int encodePos,
      byte[] encodedResult,
      int[] beta) {
    return BlockEncoder(
        data, blockIndex, blockSize, remainder, encodePos, encodedResult, beta, null, null);
  }

  public static int BlockEncoder(
      int[] data,
      int blockIndex,
      int blockSize,
      int remainder,
      int encodePos,
      byte[] encodedResult,
      int[] beta,
      long[] ts2diffTime,
      long[] subcolumnTime) {
    long ts2diffStart = System.nanoTime();
    int[] minDelta = SubcolumnPruneNewTest.borrowMinDelta3Buffer();
    int[] dataDelta = SubcolumnPruneNewTest.borrowDataDeltaBuffer();
    fillAbsDeltaTsBlock(data, blockIndex, blockSize, remainder, minDelta, dataDelta);

    encodedResult[encodePos] = (byte) (minDelta[0] >> 24);
    encodedResult[encodePos + 1] = (byte) (minDelta[0] >> 16);
    encodedResult[encodePos + 2] = (byte) (minDelta[0] >> 8);
    encodedResult[encodePos + 3] = (byte) minDelta[0];
    encodePos += 4;

    encodedResult[encodePos] = (byte) (minDelta[1] >> 24);
    encodedResult[encodePos + 1] = (byte) (minDelta[1] >> 16);
    encodedResult[encodePos + 2] = (byte) (minDelta[1] >> 8);
    encodedResult[encodePos + 3] = (byte) minDelta[1];
    encodePos += 4;

    int maxValue = 0;
    for (int v : dataDelta) {
      if (v > maxValue) {
        maxValue = v;
      }
    }
    int m = SubcolumnPruneNewTest.bitWidth(maxValue);
    int[] encodingType = SubcolumnPruneNewTest.borrowEncodingTypeBuffer();
    long ts2diffEnd = System.nanoTime();
    if (ts2diffTime != null) {
      ts2diffTime[0] += (ts2diffEnd - ts2diffStart);
    }

    long subStart = System.nanoTime();
    beta[0] =
        SubcolumnPruneNewTest.Subcolumn(dataDelta, remainder - 1, m, blockSize, encodingType);
    encodePos =
        SubcolumnPruneNewTest.SubcolumnEncoder(
            dataDelta,
            remainder - 1,
            encodePos,
            encodedResult,
            beta,
            blockSize,
            encodingType,
            m);
    long subEnd = System.nanoTime();
    if (subcolumnTime != null) {
      subcolumnTime[0] += (subEnd - subStart);
    }

    return encodePos;
  }

  public static int BlockDecoder(
      byte[] encodedResult,
      int blockIndex,
      int blockSize,
      int remainder,
      int encodePos,
      int[] data) {
    int[] minDelta = new int[3];

    minDelta[0] =
        ((encodedResult[encodePos] & 0xFF) << 24)
            | ((encodedResult[encodePos + 1] & 0xFF) << 16)
            | ((encodedResult[encodePos + 2] & 0xFF) << 8)
            | (encodedResult[encodePos + 3] & 0xFF);
    encodePos += 4;

    minDelta[1] =
        ((encodedResult[encodePos] & 0xFF) << 24)
            | ((encodedResult[encodePos + 1] & 0xFF) << 16)
            | ((encodedResult[encodePos + 2] & 0xFF) << 8)
            | (encodedResult[encodePos + 3] & 0xFF);
    encodePos += 4;

    int[] dataDelta = new int[remainder - 1];
    encodePos = SubcolumnPruneNewTest.SubcolumnDecoder(encodedResult, encodePos, dataDelta, blockSize);

    for (int i = 0; i < remainder - 1; i++) {
      dataDelta[i] = dataDelta[i] + minDelta[1];
    }

    data[blockIndex * blockSize] = minDelta[0];
    for (int i = 0; i < remainder - 1; i++) {
      data[blockIndex * blockSize + i + 1] = data[blockIndex * blockSize + i] + dataDelta[i];
    }
    return encodePos;
  }

  public static int getDecimalPrecision(String str) {
    int decimalIndex = str.indexOf(".");
    if (decimalIndex == -1) {
      return 0;
    }
    return str.substring(decimalIndex + 1).length();
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
    // String parentDir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
    String parentDir = "D:/github/xjz17/subcolumn/";

    String inputParentDir = parentDir + "dataset/";

    String outputParentDir = parentDir + "result/";
    String outputPath = outputParentDir + "ts2diff_subcolumn_adddict_prunenew_opt2.csv";

    int blockSize = 512;
    int repeatTime = 100;

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
          "Compression Ratio",
          "TS2DIFF Time",
          "Subcolumn Encode Time"
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
      ArrayList<Float> data1 = new ArrayList<>();

      int maxDecimal = 0;
      while (loader.readRecord()) {
        String fStr = loader.getValues()[0];
        if (fStr.isEmpty()) {
          continue;
        }
        int curDecimal = getDecimalPrecision(fStr);
        if (curDecimal > maxDecimal) {
          maxDecimal = curDecimal;
        }
        data1.add(Float.valueOf(fStr));
      }
      inputStream.close();

      int[] data2Arr = new int[data1.size()];
      int maxMul = (int) Math.pow(10, maxDecimal);
      for (int i = 0; i < data1.size(); i++) {
        data2Arr[i] = (int) (data1.get(i) * maxMul);
      }

      byte[] encodedResult = new byte[data2Arr.length * 8];
      long encodeTime = 0;
      long decodeTime = 0;
      long ts2diffTime = 0;
      long subcolumnEncodeTime = 0;
      double compressedSize = 0;
      int length = 0;
      long[] ts2diffTimeArr = new long[1];
      long[] subcolumnEncodeTimeArr = new long[1];

      long s = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        ts2diffTimeArr[0] = 0;
        subcolumnEncodeTimeArr[0] = 0;
        length =
            Encoder(
                data2Arr, blockSize, encodedResult, ts2diffTimeArr, subcolumnEncodeTimeArr);
        ts2diffTime += ts2diffTimeArr[0];
        subcolumnEncodeTime += subcolumnEncodeTimeArr[0];
      }
      long e = System.nanoTime();
      encodeTime += (e - s) / repeatTime;
      ts2diffTime /= repeatTime;
      subcolumnEncodeTime /= repeatTime;
      compressedSize += length;

      s = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        Decoder(encodedResult);
      }
      e = System.nanoTime();
      decodeTime += (e - s) / repeatTime;

      double ratio = compressedSize / (double) (Math.max(1, data1.size()) * Long.BYTES);
      writer.writeRecord(
          new String[] {
            datasetName,
            "TS2DIFF+Sub-columns(AddDictPruneNew-Opt2)",
            String.valueOf(encodeTime),
            String.valueOf(decodeTime),
            String.valueOf(data1.size()),
            String.valueOf(compressedSize),
            String.valueOf(ratio),
            String.valueOf(ts2diffTime),
            String.valueOf(subcolumnEncodeTime)
          });
      System.out.println(ratio);
    }

    writer.close();
  }
}
