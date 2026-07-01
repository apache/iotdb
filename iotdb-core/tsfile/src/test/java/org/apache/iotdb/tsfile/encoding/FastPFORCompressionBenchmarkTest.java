package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntCompressor;
import org.junit.Assume;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * Benchmarks {@link FastPFOR} (JavaFastPFOR, same family as FastPFOR C++ {@code fastpfor.h})
 * compression ratio and encode/decode time, following the CSV workflow in
 * {@link SubcolumnAddDictionaryTest#test0()}.
 *
 * <p>FastPFOR requires the input length to be a multiple of {@link FastPFOR#BLOCK_SIZE} (256).
 * Inputs are zero-padded for compression; ratios use the original point count in the denominator.
 */
public class FastPFORCompressionBenchmarkTest {

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

  static int paddedLength(int length, int blockSize) {
    int r = length % blockSize;
    return r == 0 ? length : length + (blockSize - r);
  }

  static int[] padForFastPFor(int[] data) {
    int m = paddedLength(data.length, FastPFOR.BLOCK_SIZE);
    if (m == data.length) {
      return data;
    }
    return Arrays.copyOf(data, m);
  }

  @Test
  public void testRoundTripPadded() {
    Random rnd = new Random(7);
    for (int n : new int[] {256, 512, 1024, 4096}) {
      int[] a = new int[n];
      for (int i = 0; i < n; i++) {
        a[i] = rnd.nextInt(1 << 18);
      }
      IntCompressor comp = new IntCompressor(new FastPFOR());
      int[] c = comp.compress(a);
      int[] b = comp.uncompress(c);
      assertEquals(a.length, b.length);
      for (int i = 0; i < a.length; i++) {
        assertEquals(a[i], b[i]);
      }
    }
  }

  @Test
  public void testRoundTripWithPadding() {
    int[] shortData = new int[100];
    for (int i = 0; i < shortData.length; i++) {
      shortData[i] = i * 3;
    }
    int[] padded = padForFastPFor(shortData);
    IntCompressor comp = new IntCompressor(new FastPFOR());
    int[] c = comp.compress(padded);
    int[] b = comp.uncompress(c);
    for (int i = 0; i < shortData.length; i++) {
      assertEquals(shortData[i], b[i]);
    }
  }

  @Test
  public void testSyntheticCompressionStats() {
    int n = 50_000;
    int nPad = paddedLength(n, FastPFOR.BLOCK_SIZE);
    int[] data = new int[nPad];
    for (int i = 0; i < n; i++) {
      data[i] = i % 17;
    }
    IntCompressor comp = new IntCompressor(new FastPFOR());
    for (int r = 0; r < REPEAT_WARMUP; r++) {
      comp.uncompress(comp.compress(data));
    }
    int[] compressed = comp.compress(data);
    long t0 = System.nanoTime();
    for (int r = 0; r < REPEAT_TIMED; r++) {
      compressed = comp.compress(data);
    }
    long encNs = (System.nanoTime() - t0) / REPEAT_TIMED;
    int[] decoded = null;
    t0 = System.nanoTime();
    for (int r = 0; r < REPEAT_TIMED; r++) {
      decoded = comp.uncompress(compressed);
    }
    long decNs = (System.nanoTime() - t0) / REPEAT_TIMED;
    for (int i = 0; i < n; i++) {
      assertEquals(data[i], decoded[i]);
    }
    double rawBytes = (double) n * Integer.BYTES;
    double compBytes = (double) compressed.length * Integer.BYTES;
    double ratio = compBytes / rawBytes;
    System.out.printf(
        "FastPFOR synthetic: n=%d (padded to %d) encode=%d ns decode=%d ns compressed=%d B ratio=%.4f (vs original n)%n",
        n, nPad, encNs, decNs, (int) compBytes, ratio);
    org.junit.Assert.assertTrue("expect reasonable size on low-entropy data", ratio < 0.5);
  }

  @Test
  public void testCsvDatasetsIfPresent() throws IOException {
    String parentDir = "path/to/your/directory/";
    String inputParentDir = parentDir + "dataset/";
    File directory = new File(inputParentDir);
    Assume.assumeTrue(
        "Skip CSV benchmark when " + inputParentDir + " is missing", directory.isDirectory());

    String outputParentDir = parentDir + "result/";
    new File(outputParentDir).mkdirs();
    String outputPath = outputParentDir + "fastpfor_compression.csv";

    CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
    writer.setRecordDelimiter('\n');
    writer.writeRecord(
        new String[] {
          "Dataset",
          "Algorithm",
          "Encode ns",
          "Decode ns",
          "Points",
          "Padded to",
          "Compressed bytes",
          "Ratio (compressed / raw int32 for original points)"
        });

    IntCompressor comp = new IntCompressor(new FastPFOR());
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
      int[] raw = new int[floats.size()];
      int maxMul = (int) Math.pow(10, maxDecimal);
      for (int i = 0; i < floats.size(); i++) {
        raw[i] = (int) (floats.get(i) * maxMul);
      }
      int[] padded = padForFastPFor(raw);

      for (int r = 0; r < REPEAT_WARMUP; r++) {
        comp.uncompress(comp.compress(padded));
      }
      int[] compressed = comp.compress(padded);
      long s = System.nanoTime();
      for (int r = 0; r < REPEAT_TIMED; r++) {
        compressed = comp.compress(padded);
      }
      long encNs = (System.nanoTime() - s) / REPEAT_TIMED;

      int[] decoded = comp.uncompress(compressed);
      s = System.nanoTime();
      for (int r = 0; r < REPEAT_TIMED; r++) {
        decoded = comp.uncompress(compressed);
      }
      long decNs = (System.nanoTime() - s) / REPEAT_TIMED;

      for (int i = 0; i < raw.length; i++) {
        assertEquals(raw[i], decoded[i]);
      }
      int compBytes = compressed.length * Integer.BYTES;
      double ratio = compBytes / (double) (raw.length * (long) Integer.BYTES);
      writer.writeRecord(
          new String[] {
            datasetName,
            "FastPFOR",
            Long.toString(encNs),
            Long.toString(decNs),
            Integer.toString(raw.length),
            Integer.toString(padded.length),
            Integer.toString(compBytes),
            Double.toString(ratio)
          });
      System.out.printf("%s FastPFOR ratio=%.4f (padded %d -> %d)%n", datasetName, ratio, raw.length, padded.length);
    }
    writer.close();
  }
}
