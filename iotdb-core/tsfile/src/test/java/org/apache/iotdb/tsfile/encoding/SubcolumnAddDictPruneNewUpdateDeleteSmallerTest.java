package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SubcolumnAddDictPruneNewUpdateDeleteSmallerTest {

  @Test
  public void test0() throws IOException {
    String parentDir = "D://github/xjz17/subcolumn/";
    String inputParentDir = parentDir + "dataset/";
    String outputParentDir = parentDir + "result/update/";
    String outputPath = outputParentDir + "subcolumn_adddict_prunenew_update_delete_smaller.csv";

    int blockSize = 512;
    int repeatTime = 200;

    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Encoding Algorithm",
          "Encoding Time",
          "Delete Time with Sub-column",
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
      if (origin.length <= 1) {
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

      int[] deleted = new int[origin.length - 1];
      System.arraycopy(origin, 0, deleted, 0, deleted.length);

      long deleteCompressTime;
      int updatedLength = encodedLength;
      int[] beta = new int[] {2};
      int newRemainder = tailInfo.remainder - 1;
      start = System.nanoTime();
      for (int repeat = 0; repeat < repeatTime; repeat++) {
        if (newRemainder == 0) {
          updatedLength = tailInfo.tailStartPos;
          continue;
        }
        int encodePos = tailInfo.tailStartPos;
        if (newRemainder <= 3) {
          int base = tailInfo.numBlocks * blockSize;
          for (int i = 0; i < newRemainder; i++) {
            SubcolumnPruneNewTest.int2Bytes(deleted[base + i], encodePos, encodedResult);
            encodePos += 4;
          }
        } else {
          encodePos =
              SubcolumnPruneNewTest.BlockEncoder(
                  deleted,
                  tailInfo.numBlocks,
                  blockSize,
                  newRemainder,
                  encodePos,
                  encodedResult,
                  beta);
        }
        updatedLength = encodePos;
      }
      end = System.nanoTime();
      deleteCompressTime = (end - start) / repeatTime;

      double compressionRatio = encodedLength / (double) (origin.length * Long.BYTES);
      writer.writeRecord(
          new String[] {
            datasetName,
            "SubcolumnAddDictPruneNew",
            String.valueOf(encodeTime),
            String.valueOf(deleteCompressTime),
            String.valueOf(origin.length),
            String.valueOf(tailInfo.remainder),
            String.valueOf(updatedLength),
            String.valueOf(compressionRatio)
          });
    }
    writer.close();
  }
}
