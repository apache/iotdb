package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Benchmarks Simple8b (FastPFOR {@code simple8b.h}, MarkLength=true) compression ratio and
 * encode/decode time, following the CSV workflow in {@link SubcolumnAddDictionaryTest#test0()}.
 *
 * <p>JavaFastPFOR does not ship Simple8b; this test uses {@link FastPForSimple8bCodec}, a Java
 * port of the same algorithm.
 */
public class Simple8bCompressionBenchmarkTest {

  private static final int REPEAT_WARMUP = 5;
  private static final int REPEAT_TIMED = 200;

  public static int getDecimalPrecision(String str) {
    int decimalIndex = str.indexOf('.');
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
  public void testRoundTripSmall() {
    int[] a = {0, 1, 2, 3, 100, 1000, 0xffff, 0};
    int[] c = FastPForSimple8bCodec.encode(a);
    int[] b = FastPForSimple8bCodec.decode(c);
    assertArrayEquals(a, b);
  }

  @Test
  public void testRoundTripRandom() {
    Random rnd = new Random(42);
    for (int n : new int[] {1, 7, 59, 120, 239, 240, 241, 500, 4000}) {
      int[] a = new int[n];
      for (int i = 0; i < n; i++) {
        a[i] = rnd.nextInt(1 << 20);
      }
      int[] c = FastPForSimple8bCodec.encode(a);
      int[] b = FastPForSimple8bCodec.decode(c);
      assertArrayEquals(a, b);
    }
  }

  @Test
  public void testSyntheticCompressionStats() {
    int n = 50_000;
    int[] data = new int[n];
    for (int i = 0; i < n; i++) {
      data[i] = i % 17;
    }
    long t0 = System.nanoTime();
    int[] compressed = null;
    for (int r = 0; r < REPEAT_TIMED; r++) {
      compressed = FastPForSimple8bCodec.encode(data);
    }
    long encNs = (System.nanoTime() - t0) / REPEAT_TIMED;
    int[] decoded = null;
    t0 = System.nanoTime();
    for (int r = 0; r < REPEAT_TIMED; r++) {
      decoded = FastPForSimple8bCodec.decode(compressed);
    }
    long decNs = (System.nanoTime() - t0) / REPEAT_TIMED;
    assertArrayEquals(data, decoded);
    double rawBytes = (double) n * Integer.BYTES;
    double compBytes = FastPForSimple8bCodec.compressedSizeBytes(compressed);
    double ratio = compBytes / rawBytes;
    System.out.printf(
        "Simple8b synthetic: n=%d encode=%d ns decode=%d ns compressed=%d B ratio=%.4f%n",
        n, encNs, decNs, (int) compBytes, ratio);
    assertTrue("compression should not expand catastrophically on low-entropy ints", ratio < 1.2);
  }

  /**
   * Same structure as {@link SubcolumnAddDictionaryTest#test0()}: read float CSVs, scale to int[],
   * measure encode/decode time and compression ratio, write a result CSV. Skips if the dataset
   * directory does not exist.
   */
  @Test
  public void testCsvDatasetsIfPresent() throws IOException {
    String parentDir = "path/to/your/directory/";
    String inputParentDir = parentDir + "dataset/";
    File directory = new File(inputParentDir);
    Assume.assumeTrue(
        "Skip CSV benchmark when " + inputParentDir + " is missing", directory.isDirectory());

    String outputParentDir = parentDir + "result/";
    new File(outputParentDir).mkdirs();
    String outputPath = outputParentDir + "simple8b_compression.csv";

    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Algorithm",
          "Encode ns",
          "Decode ns",
          "Points",
          "Compressed bytes",
          "Ratio (compressed / raw int32)"
        });

    File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
    Assume.assumeTrue(csvFiles != null);
    for (File file : csvFiles) {
      String datasetName = extractFileName(file.toString());
      InputStream inputStream = Files.newInputStream(file.toPath());
      CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
      ArrayList<Float> floats = new ArrayList<>();
      int maxDecimal = 0;
      while (loader.readRecord()) {
        String fStr = loader.getValues()[0];
        if (fStr.isEmpty()) {
          continue;
        }
        maxDecimal = Math.max(maxDecimal, getDecimalPrecision(fStr));
        floats.add(Float.valueOf(fStr));
      }
      inputStream.close();
      if (maxDecimal > 8) {
        maxDecimal = 8;
      }
      int[] data = new int[floats.size()];
      int maxMul = (int) Math.pow(10, maxDecimal);
      for (int i = 0; i < floats.size(); i++) {
        data[i] = (int) (floats.get(i) * maxMul);
      }

      for (int r = 0; r < REPEAT_WARMUP; r++) {
        FastPForSimple8bCodec.decode(FastPForSimple8bCodec.encode(data));
      }
      int[] compressed = FastPForSimple8bCodec.encode(data);
      long s = System.nanoTime();
      for (int r = 0; r < REPEAT_TIMED; r++) {
        compressed = FastPForSimple8bCodec.encode(data);
      }
      long encNs = (System.nanoTime() - s) / REPEAT_TIMED;

      int[] decoded = FastPForSimple8bCodec.decode(compressed);
      s = System.nanoTime();
      for (int r = 0; r < REPEAT_TIMED; r++) {
        decoded = FastPForSimple8bCodec.decode(compressed);
      }
      long decNs = (System.nanoTime() - s) / REPEAT_TIMED;

      assertArrayEquals(data, decoded);
      int compBytes = FastPForSimple8bCodec.compressedSizeBytes(compressed);
      double ratio = compBytes / (double) (data.length * (long) Integer.BYTES);
      writer.writeRecord(
          new String[] {
            datasetName,
            "Simple8b",
            Long.toString(encNs),
            Long.toString(decNs),
            Integer.toString(data.length),
            Integer.toString(compBytes),
            Double.toString(ratio)
          });
      System.out.printf("%s Simple8b ratio=%.4f%n", datasetName, ratio);
    }
    writer.close();
  }
}
