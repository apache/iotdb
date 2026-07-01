package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SubcolumnAddDictPruneNewUpdateLargerTest {

  @Test
  public void test0() throws IOException {
    String parentDir = "D://github/xjz17/subcolumn/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/update/";
    String outputPath = outputParentDir + "subcolumn_adddict_prunenew_update_larger.csv";

    int blockSize = 512;
    int repeatTime = 200;

    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Encoding Time",
          "Insert Time with Sub-column",
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
      if (origin.length == 0) {
        continue;
      }

      byte[] encodedResult = new byte[Math.max(16, origin.length * 12)];
      long start = System.nanoTime();
      int encodedLength = 0;
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        encodedLength = SubcolumnPruneNewTest.Encoder(origin, blockSize, encodedResult);
      }
      long end = System.nanoTime();
      long encodeTime = (end - start) / repeatTime;
      encodedLength = SubcolumnPruneNewTest.Encoder(origin, blockSize, encodedResult);

      SubcolumnAddDictPruneNewUpdateTestUtil.TailInfo tailInfo =
          SubcolumnAddDictPruneNewUpdateTestUtil.locateTailInfo(encodedResult, encodedLength);
      if (tailInfo.remainder <= 0) {
        continue;
      }

      int updateIndex = tailInfo.numBlocks * blockSize + tailInfo.remainder - 1;
      int updatedValue = (stats.maxValue == Integer.MAX_VALUE) ? stats.maxValue : stats.maxValue + 1;
      int[] updated = new int[origin.length];
      System.arraycopy(origin, 0, updated, 0, origin.length);
      updated[updateIndex] = updatedValue;

      long updateCompressTime;
      int updatedLength = encodedLength;
      int[] beta = new int[] {2};
      start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        int encodePos = tailInfo.tailStartPos;
        if (tailInfo.remainder <= 3) {
          int base = tailInfo.numBlocks * blockSize;
          for (int i = 0; i < tailInfo.remainder; i++) {
            SubcolumnPruneNewTest.int2Bytes(updated[base + i], encodePos, encodedResult);
            encodePos += 4;
          }
        } else {
          encodePos =
              SubcolumnPruneNewTest.BlockEncoder(
                  updated,
                  tailInfo.numBlocks,
                  blockSize,
                  tailInfo.remainder,
                  encodePos,
                  encodedResult,
                  beta);
        }
        updatedLength = encodePos;
      }
      end = System.nanoTime();
      updateCompressTime = (end - start) / repeatTime;

      double compressionRatio = encodedLength / (double) (origin.length * Long.BYTES);
      writer.writeRecord(
          new String[] {
            datasetName,
            "SubcolumnAddDictPruneNew",
            String.valueOf(encodeTime),
            String.valueOf(updateCompressTime),
            String.valueOf(origin.length),
            String.valueOf(tailInfo.remainder),
            String.valueOf(updatedLength),
            String.valueOf(compressionRatio)
          });
    }
    writer.close();
  }
}
