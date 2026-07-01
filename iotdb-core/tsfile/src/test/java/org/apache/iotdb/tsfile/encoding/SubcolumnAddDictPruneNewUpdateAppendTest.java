package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SubcolumnAddDictPruneNewUpdateAppendTest {

  @Test
  public void test0() throws IOException {
    String parentDir = "D://github/xjz17/subcolumn/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/update/";
    String outputPath = outputParentDir + "subcolumn_adddict_prunenew_append.csv";

    int blockSize = 512;
    int repeatTime = 200;

    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Encoding Time",
          "Append-only Time with Sub-column",
          "Points",
          "Remaining Points",
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
      String datasetName = SubcolumnAddDictPruneNewUpdateTestUtil.extractFileName(file.toString());
      if (datasetName.equals("POI-lon") || datasetName.equals("POI-lat")) {
        continue;
      }
      System.out.println(datasetName);

      SubcolumnAddDictPruneNewUpdateTestUtil.DataStats stats =
          SubcolumnAddDictPruneNewUpdateTestUtil.loadDatasetAsIntArray(file);
      int[] origin = stats.values;

      int appendValue = (stats.maxValue == Integer.MAX_VALUE) ? stats.maxValue : (stats.maxValue + 1);

      int[] appended = new int[origin.length + 1];
      System.arraycopy(origin, 0, appended, 0, origin.length);
      appended[origin.length] = appendValue;

      byte[] encodedResult = new byte[Math.max(16, appended.length * 12)];
      int encodedLength;

      long start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        encodedLength = SubcolumnPruneNewTest.Encoder(origin, blockSize, encodedResult);
      }
      long end = System.nanoTime();
      long encodeTime = (end - start) / repeatTime;
      encodedLength = SubcolumnPruneNewTest.Encoder(origin, blockSize, encodedResult);

      SubcolumnAddDictPruneNewUpdateTestUtil.TailInfo tailInfo =
          SubcolumnAddDictPruneNewUpdateTestUtil.locateTailInfo(encodedResult, encodedLength);

      long appendCompressTime;
      int appendedLength = encodedLength;
      int[] beta = new int[] {2};
      start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        int remainder = tailInfo.remainder;
        int numBlocks = tailInfo.numBlocks;
        int tailStart = tailInfo.tailStartPos;
        int newRemainder = remainder + 1;

        int encodePos = tailStart;
        if (newRemainder <= 3) {
          int base = numBlocks * blockSize;
          for (int i = 0; i < newRemainder; i++) {
            SubcolumnPruneNewTest.int2Bytes(appended[base + i], encodePos, encodedResult);
            encodePos += 4;
          }
        } else {
          encodePos =
              SubcolumnPruneNewTest.BlockEncoder(
                  appended, numBlocks, blockSize, newRemainder, encodePos, encodedResult, beta);
        }
        appendedLength = encodePos;
      }
      end = System.nanoTime();
      appendCompressTime = (end - start) / repeatTime;

      double compressionRatio = encodedLength / (double) (origin.length * Long.BYTES);
      writer.writeRecord(
          new String[] {
            datasetName,
            "SubcolumnPruneNew",
            String.valueOf(encodeTime),
            String.valueOf(appendCompressTime),
            String.valueOf(origin.length),
            String.valueOf(tailInfo.remainder),
            String.valueOf(appendedLength),
            String.valueOf(compressionRatio)
          });
    }

    writer.close();
  }
}
