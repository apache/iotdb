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

public class BPUpdateSmallerTest {
  private static int skipBlock(byte[] encodedResult, int encodePos, int rowCount) {
    encodePos += 4;
    int[] tmp = new int[rowCount];
    return BPTest.BPDecoder(encodedResult, encodePos, tmp);
  }

  @Test
  public void test0() throws IOException {
    String parentDir = "D://github/xjz17/subcolumn/";
    String inputParentDir = parentDir + "dataset/";
    String outputPath = parentDir + "result/update/bp_update_smaller.csv";
    int blockSize = 512, repeatTime = 200;
    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(new String[]{"Dataset","Encoding Algorithm","Encoding Time","Insert Time with Sub-column","Points","Remaining Points","Compressed Size","Compression Ratio"});
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
      int min = Integer.MAX_VALUE;
      for (int i = 0; i < data.size(); i++) { origin[i] = (int) (data.get(i) * mul); min = Math.min(min, origin[i]); }
      if (origin.length == 0) continue;
      byte[] encoded = new byte[Math.max(16, origin.length * 8)];
      int len = 0;
      long s = System.nanoTime();
      for (int r = 0; r < repeatTime; r++) len = BPTest.Encoder(origin, blockSize, encoded);
      long e = System.nanoTime();
      long encodeTime = (e - s) / repeatTime;
      int num = origin.length / blockSize, rem = origin.length % blockSize;
      if (rem <= 0) continue;
      int[] updated = new int[origin.length];
      System.arraycopy(origin, 0, updated, 0, origin.length);
      updated[num * blockSize + rem - 1] = min == Integer.MIN_VALUE ? min : min - 1;
      int tail = 8; for (int i=0;i<num;i++) tail = skipBlock(encoded, tail, blockSize);
      int updatedLen = len;
      s = System.nanoTime();
      for (int r = 0; r < repeatTime; r++) updatedLen = BPTest.BlockEncoder(updated, num, blockSize, rem, tail, encoded);
      e = System.nanoTime();
      long t = (e - s) / repeatTime;
      double ratio = len / (double)(Math.max(1,origin.length)*Long.BYTES);
      writer.writeRecord(new String[]{name,"BP",String.valueOf(encodeTime),String.valueOf(t),String.valueOf(origin.length),String.valueOf(rem),String.valueOf(updatedLen),String.valueOf(ratio)});
    }
    writer.close();
  }
}
