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

public class BPUpdateDeleteSmallerTest {
  @Test
  public void test0() throws IOException {
    String parentDir = "D://github/xjz17/subcolumn/";
    String inputParentDir = parentDir + "dataset/";
    String outputPath = parentDir + "result/update/bp_update_delete_smaller.csv";
    int blockSize = 512, repeatTime = 200;
    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(new String[]{"Dataset","Encoding Algorithm","Encoding Time","Delete Time with Sub-column","Points","Remaining Points","Compressed Size","Compression Ratio"});
    File[] csvFiles = new File(inputParentDir).listFiles((dir, name) -> name.endsWith(".csv"));
    if (csvFiles == null) { writer.close(); return; }
    for (File file : csvFiles) {
      String name = BPTest.extractFileName(file.toString());
      if (name.equals("POI-lon") || name.equals("POI-lat")) continue;
      InputStream is = Files.newInputStream(file.toPath());
      CsvReader loader = new CsvReader(is, StandardCharsets.UTF_8);
      ArrayList<Float> data = new ArrayList<>();
      int dec = 0;
      while (loader.readRecord()) { String s = loader.getValues()[0]; if (s.isEmpty()) continue; dec = Math.max(dec, BPTest.getDecimalPrecision(s)); data.add(Float.valueOf(s)); }
      is.close();
      int mul = (int) Math.pow(10, Math.min(dec, 8));
      int[] origin = new int[data.size()];
      for (int i = 0; i < data.size(); i++) origin[i] = (int) (data.get(i) * mul);
      if (origin.length <= 1) continue;
      byte[] encoded = new byte[Math.max(16, origin.length * 8)];
      int len = 0;
      long s = System.nanoTime();
      for (int r = 0; r < repeatTime; r++) len = BPTest.Encoder(origin, blockSize, encoded);
      long e = System.nanoTime();
      long encodeTime = (e - s) / repeatTime;
      int num = origin.length / blockSize, rem = origin.length % blockSize;
      if (rem <= 0) continue;
      int[] deleted = new int[origin.length - 1];
      System.arraycopy(origin, 0, deleted, 0, deleted.length);
      int tailStart = 8;
      for (int i = 0; i < num; i++) {
        tailStart += 4;
        int[] tmp = new int[blockSize];
        tailStart = BPTest.BPDecoder(encoded, tailStart, tmp);
      }
      int newRem = rem - 1;
      int updatedLen = len;
      s = System.nanoTime();
      for (int r = 0; r < repeatTime; r++) {
        if (newRem == 0) {
          updatedLen = tailStart;
        } else {
          updatedLen = BPTest.BlockEncoder(deleted, num, blockSize, newRem, tailStart, encoded);
        }
      }
      e = System.nanoTime();
      long t = (e - s) / repeatTime;
      double ratio = len / (double)(Math.max(1,origin.length)*Long.BYTES);
      writer.writeRecord(new String[]{name,"BP",String.valueOf(encodeTime),String.valueOf(t),String.valueOf(origin.length),String.valueOf(rem),String.valueOf(updatedLen),String.valueOf(ratio)});
    }
    writer.close();
  }
}
