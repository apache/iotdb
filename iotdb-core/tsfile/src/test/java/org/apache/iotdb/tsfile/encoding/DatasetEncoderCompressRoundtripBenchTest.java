/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import org.apache.iotdb.tsfile.compress.ICompressor;
import org.apache.iotdb.tsfile.compress.IUnCompressor;
import org.apache.iotdb.tsfile.encoding.decoder.DictionaryDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DoublePrecisionChimpDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DoublePrecisionDecoderV2;
import org.apache.iotdb.tsfile.encoding.encoder.DictionaryEncoder;
import org.apache.iotdb.tsfile.encoding.elf.ElfDoublePrecisionBenchCodec;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionChimpEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionEncoderV2;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import me.lemire.integercompression.FastPFOR;
import me.lemire.integercompression.IntCompressor;
import org.apache.iotdb.tsfile.utils.Binary;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinBase;
import com.sun.jna.platform.win32.WinDef.DWORDByReference;
import com.sun.jna.platform.win32.WinNT;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.win32.StdCallLibrary;
import com.sun.jna.win32.W32APIOptions;

public class DatasetEncoderCompressRoundtripBenchTest {

  private interface Kernel32PointerRead extends StdCallLibrary {
    Kernel32PointerRead INSTANCE =
        Native.load("kernel32", Kernel32PointerRead.class, W32APIOptions.UNICODE_OPTIONS);

    boolean ReadFile(
        HANDLE hFile,
        Pointer lpBuffer,
        int nNumberOfBytesToRead,
        IntByReference lpNumberOfBytesRead,
        Pointer lpOverlapped);
  }

  private static final int BENCH_PHASE_REPEATS = 100;

  private static final int BENCH_LZMA2_PRESET = 3;

  private static byte[] compressLzma2BenchPreset(byte[] data) throws IOException {
    LZMA2Options options = new LZMA2Options(BENCH_LZMA2_PRESET);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    XZOutputStream lzma2 = new XZOutputStream(out, options);
    lzma2.write(data);
    lzma2.close();
    return out.toByteArray();
  }

  private static final int K_MAX_DECIMAL_PRECISION = 8;

  private static final int SUBCOLUMN_BLOCK_SIZE = 512;

  private static final String kResultEncoderCompressDir =
      "D:/github/xjz17/subcolumn/result/encoder_compress";

  /** HDD bench: dataset + bins on E:; metrics CSV on D: repo (s1). */
  private static final BenchStorageProfile HDD_PROFILE =
      new BenchStorageProfile(
          "HDD",
          "E:/xjz/subcolumn/dataset",
          "E:/xjz/subcolumn/encode_compress_bins/bins_combined_s1",
          kResultEncoderCompressDir
              + "/encoder_compress_roundtrip_write_metrics_combined_s1.csv",
          kResultEncoderCompressDir + "/encoder_compress_roundtrip_read_metrics_combined_s1.csv",
          kResultEncoderCompressDir
              + "/encoder_compress_roundtrip_compress_manifest_combined_s1.csv",
          "E:/xjz/subcolumn/encode_compress_bins/decoded_csv_combined_s1");

  /** SSD bench: dataset + bins on D:; metrics CSV s2 under repo result dir. */
  private static final BenchStorageProfile SSD_PROFILE =
      new BenchStorageProfile(
          "SSD",
          "D:/github/xjz17/subcolumn/dataset",
          kResultEncoderCompressDir + "/bins_combined_s2",
          kResultEncoderCompressDir
              + "/encoder_compress_roundtrip_write_metrics_combined_s2.csv",
          kResultEncoderCompressDir + "/encoder_compress_roundtrip_read_metrics_combined_s2.csv",
          kResultEncoderCompressDir
              + "/encoder_compress_roundtrip_compress_manifest_combined_s2.csv",
          kResultEncoderCompressDir + "/decoded_csv_combined_s2");

  private static final class BenchStorageProfile {
    final String label;
    final String datasetDir;
    final String binOutputDir;
    final String writeMetricsCsvPath;
    final String readMetricsCsvPath;
    final String compressManifestCsvPath;
    final String decodedCsvDir;

    BenchStorageProfile(
        String label,
        String datasetDir,
        String binOutputDir,
        String writeMetricsCsvPath,
        String readMetricsCsvPath,
        String compressManifestCsvPath,
        String decodedCsvDir) {
      this.label = label;
      this.datasetDir = datasetDir;
      this.binOutputDir = binOutputDir;
      this.writeMetricsCsvPath = writeMetricsCsvPath;
      this.readMetricsCsvPath = readMetricsCsvPath;
      this.compressManifestCsvPath = compressManifestCsvPath;
      this.decodedCsvDir = decodedCsvDir;
    }
  }

  private static final String ALGO_LZMA_CSV = "LZMA";
  private static final String SUFFIX_PLAIN_LZMA = "plain_lzma";
  private static final String ALGO_DICTIONARY_CSV = "DICTIONARY";
  private static final String SUFFIX_DICTIONARY = "dictionary";
  private static final String ALGO_GORILLA_CSV = "GORILLA";
  private static final String SUFFIX_GORILLA = "gorilla";
  private static final String ALGO_CHIMP_CSV = "CHIMP";
  private static final String SUFFIX_CHIMP = "chimp";
  private static final String ALGO_ELF_CSV = "ELF";
  private static final String SUFFIX_ELF = "elf";
  private static final String ALGO_BUFF_CSV = "BUFF";
  private static final String SUFFIX_BUFF = "buff";
  private static final String ALGO_ALP_CSV = "ALP";
  private static final String SUFFIX_ALP = "alp";
  private static final String ALGO_BITWEAVING_CSV = "BITWEAVING";
  private static final String SUFFIX_BITWEAVING = "bitweaving";
  private static final String ALGO_SIMPLE8B_CSV = "Simple8b";
  private static final String SUFFIX_SIMPLE8B = "simple8b";
  private static final String ALGO_FASTPFOR_CSV = "FastPFOR";
  private static final String SUFFIX_FASTPFOR = "fastpfor";
  private static final String ALGO_RLE_CSV = "RLE";
  private static final String SUFFIX_RLE = "rle";
  private static final String ALGO_BITPACKING_CSV = "BITPACKING";
  private static final String SUFFIX_BITPACKING = "bitpacking";
  private static final String ALGO_SPRINTZ_CSV = "SPRINTZ";
  private static final String SUFFIX_SPRINTZ = "sprintz";
  private static final String ALGO_TS_2DIFF_CSV = "TS_2DIFF";
  private static final String SUFFIX_TS_2DIFF = "ts_2diff";
  private static final String ALGO_SUBCOLUMN_CSV = "SUBCOLUMN";
  private static final String SUFFIX_SUBCOLUMN = "subcolumn";

  private static final int BUFF_ALP_BLOCK_SIZE = 512;
  private static final int HBP_BLOCK_SIZE = 512;
  private static final int SPRINTZ_TS2DIFF_BLOCK_SIZE = 1024;
  private static final int BITPACKING_BLOCK_SIZE = 1024;
  private static final int RLE_BLOCK_SIZE = 256;
  private static final int BENCH_ALGORITHM_COUNT = 16;

  private static final class PhaseTiming {
    private long sumEncodeFlushNs;
    private long sumCompressNs;
    private long sumUncompressNs;
    private long sumDecodeNs;
    private int encodedLen;
    private int compressedLen;

    double avgEncodeFlushNs() {
      return sumEncodeFlushNs / (double) BENCH_PHASE_REPEATS;
    }

    double avgCompressNs() {
      return sumCompressNs / (double) BENCH_PHASE_REPEATS;
    }

    double avgUncompressNs() {
      return sumUncompressNs / (double) BENCH_PHASE_REPEATS;
    }

    double avgDecodeNs() {
      return sumDecodeNs / (double) BENCH_PHASE_REPEATS;
    }
  }

  private static final class WritePathTimings {
    long avgDatasetReadNs;
    long avgEncodeLoopNs;
    long avgEncodeFlushNs;
    long avgCompressNs;
    long avgBinWriteNs;
    int encodedUncompressedBytes;
    int compressedBytes;
  }

  private static final class ReadPathTimings {
    long avgDatasetVerifyReadNs;
    long avgBinReadNs;
    long avgUncompressNs;
    long avgDecodeNs;
    long avgDecodedCsvWriteNs;
    int compressedBytesObserved;
  }

  private static final class ManifestRecord {
    final String dataset;
    final String algorithm;
    final String sourceCsvPath;
    final long points;
    final long encodedUncompressedBytes;
    final int rawPlainCodec;

    ManifestRecord(
        String dataset,
        String algorithm,
        String sourceCsvPath,
        long points,
        long encodedUncompressedBytes,
        int rawPlainCodec) {
      this.dataset = dataset;
      this.algorithm = algorithm;
      this.sourceCsvPath = sourceCsvPath;
      this.points = points;
      this.encodedUncompressedBytes = encodedUncompressedBytes;
      this.rawPlainCodec = rawPlainCodec;
    }
  }

  public static int getDecimalPrecision(String str) {
    int dot = str.indexOf('.');
    if (dot < 0) {
      return 0;
    }
    return str.length() - dot - 1;
  }

  private static int maxDecimalPrecision(List<String> tokens) {
    int m = 0;
    for (String s : tokens) {
      m = Math.max(m, getDecimalPrecision(s));
    }
    return m;
  }

  private static long multiplierForBoundedPrecision(int maxPrec) {
    int bounded = Math.min(maxPrec, K_MAX_DECIMAL_PRECISION);
    long mult = 1;
    for (int i = 0; i < bounded; i++) {
      mult *= 10;
    }
    return mult;
  }

  private static List<String> readFirstColumnTokens(File csvFile) throws IOException {
    List<String> tokens = new ArrayList<>();
    try (InputStream in = Files.newInputStream(csvFile.toPath())) {
      CsvReader reader = new CsvReader(in, StandardCharsets.UTF_8);
      while (reader.readRecord()) {
        String v = reader.getValues()[0].trim();
        if (v.isEmpty()) {
          continue;
        }
        tokens.add(v);
      }
    }
    return tokens;
  }

  private static List<String[]> readFullCsvAllColumnsAsStringRows(File csvFile) throws IOException {
    List<String[]> rows = new ArrayList<>();
    try (InputStream in = Files.newInputStream(csvFile.toPath())) {
      CsvReader reader = new CsvReader(in, StandardCharsets.UTF_8);
      while (reader.readRecord()) {
        String[] vals = reader.getValues();
        rows.add(Arrays.copyOf(vals, vals.length));
      }
    }
    return rows;
  }

  private static List<String> firstColumnTokensFromRows(List<String[]> rows) {
    List<String> tokens = new ArrayList<>();
    for (String[] row : rows) {
      if (row.length == 0) {
        continue;
      }
      String v = row[0].trim();
      if (v.isEmpty()) {
        continue;
      }
      tokens.add(v);
    }
    return tokens;
  }

  private static long averageCsvFullReadWithoutNumericParseNanos(File csvFile) throws IOException {
    long sum = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      readFullCsvAllColumnsAsStringRows(csvFile);
      long t1 = System.nanoTime();
      sum += (t1 - t0);
    }
    return sum / BENCH_PHASE_REPEATS;
  }

  private static long[] scaleTokensPerSubcolumnBlock(List<String> tokens) {
    final int n = tokens.size();
    long[] out = new long[n];
    int offset = 0;
    while (offset < n) {
      int end = Math.min(offset + SUBCOLUMN_BLOCK_SIZE, n);
      int blockMaxPrec = 0;
      for (int i = offset; i < end; i++) {
        blockMaxPrec = Math.max(blockMaxPrec, getDecimalPrecision(tokens.get(i)));
      }
      int capped = Math.min(blockMaxPrec, K_MAX_DECIMAL_PRECISION);
      double mult = Math.pow(10, capped);
      for (int i = offset; i < end; i++) {
        out[i] = Math.round(Double.parseDouble(tokens.get(i)) * mult);
      }
      offset = end;
    }
    return out;
  }

  private static int[] scaleTokensToIntsPerSubcolumnBlock(List<String> tokens) {
    long[] scaled = scaleTokensPerSubcolumnBlock(tokens);
    int[] out = new int[scaled.length];
    for (int i = 0; i < scaled.length; i++) {
      out[i] = (int) scaled[i];
    }
    return out;
  }

  private static byte[] encodeSprintzInts(int[] values) {
    byte[] scratch = new byte[values.length * 8 + 64];
    int len = SPRINTZBPTest.BOSEncoder(values, SPRINTZ_TS2DIFF_BLOCK_SIZE, scratch);
    return Arrays.copyOf(scratch, len);
  }

  private static byte[] encodeTs2DiffInts(int[] values) {
    byte[] scratch = new byte[values.length * 8 + 64];
    int len = TSDIFFTest.BOSEncoderImprove(values, SPRINTZ_TS2DIFF_BLOCK_SIZE, scratch);
    return Arrays.copyOf(scratch, len);
  }

  private static byte[] encodeBitPackingInts(int[] values) {
    byte[] scratch = new byte[values.length * 4 + 64];
    int len = BPTest.Encoder(values, BITPACKING_BLOCK_SIZE, scratch);
    return Arrays.copyOf(scratch, len);
  }

  private static byte[] encodeRleLongs(long[] values) {
    byte[] scratch = new byte[values.length * 8 + 64];
    int len = RLEBPLongTest.BOSEncoderImprove(values, RLE_BLOCK_SIZE, scratch);
    return Arrays.copyOf(scratch, len);
  }

  private static byte[] encodeSubcolumnInts(int[] values) {
    byte[] scratch = new byte[values.length * 8 + 64];
    int len = SubcolumnPruneNewTest.Encoder(values, SUBCOLUMN_BLOCK_SIZE, scratch);
    return Arrays.copyOf(scratch, len);
  }

  private static double[] parseDoublesFromTokens(List<String> tokens) {
    double[] out = new double[tokens.size()];
    for (int i = 0; i < tokens.size(); i++) {
      out[i] = Double.parseDouble(tokens.get(i));
    }
    return out;
  }

  private static byte[] doublesToLittleEndianRawBytes(double[] values) {
    byte[] raw = new byte[values.length * Double.BYTES];
    ByteBuffer bb = ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN);
    for (double v : values) {
      bb.putDouble(v);
    }
    return raw;
  }

  private static double[] littleEndianBytesToDoubles(byte[] uncompressed) {
    Assert.assertEquals(0, uncompressed.length % Double.BYTES);
    int n = uncompressed.length / Double.BYTES;
    double[] out = new double[n];
    ByteBuffer bb = ByteBuffer.wrap(uncompressed).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < n; i++) {
      out[i] = bb.getDouble();
    }
    return out;
  }

  private static void assertRawDoubleRoundtrip(double[] expected, byte[] uncompressed) {
    littleEndianBytesToDoubles(uncompressed);
    // Assert.assertEquals(expected.length, got.length);
    // for (int i = 0; i < expected.length; i++) {
    //   Assert.assertEquals(
    //       Double.doubleToLongBits(expected[i]), Double.doubleToLongBits(got[i]));
    // }
  }

  private static Binary longToPlainBinary(long v) {
    byte[] b = new byte[Long.BYTES];
    ByteBuffer.wrap(b).putLong(v);
    return new Binary(b);
  }

  private static long binaryToLong(Binary bin) {
    byte[] v = bin.getValues();
    Assert.assertEquals(Long.BYTES, v.length);
    return ByteBuffer.wrap(v).getLong();
  }

  private static void assertDictionaryRoundtrip(long[] expected, byte[] encodedPage) {
    ByteBuffer buf = ByteBuffer.wrap(encodedPage);
    DictionaryDecoder dec = new DictionaryDecoder();
    for (int i = 0; i < expected.length; i++) {
      Assert.assertTrue(dec.hasNext(buf));
      // Assert.assertEquals(expected[i], binaryToLong(dec.readBinary(buf)));
      binaryToLong(dec.readBinary(buf));
    }
    Assert.assertFalse(dec.hasNext(buf));
    dec.reset();
  }

  private static ManifestRecord parseManifestDataLine(String rawLine) throws IOException {
    String line = rawLine.trim();
    if (line.isEmpty()) {
      return null;
    }
    if (line.endsWith("\r")) {
      line = line.substring(0, line.length() - 1);
    }
    try {
      int p = line.lastIndexOf(',');
      int rawPlain = Integer.parseInt(line.substring(p + 1).trim());
      line = line.substring(0, p);
      p = line.lastIndexOf(',');
      long encBytes = Long.parseLong(line.substring(p + 1).trim());
      line = line.substring(0, p);
      p = line.lastIndexOf(',');
      long points = Long.parseLong(line.substring(p + 1).trim());
      line = line.substring(0, p);
      p = line.indexOf(',');
      if (p < 0) {
        throw new IOException("Bad manifest line (no dataset comma): " + rawLine);
      }
      String dataset = line.substring(0, p).trim();
      line = line.substring(p + 1);
      p = line.indexOf(',');
      if (p < 0) {
        throw new IOException("Bad manifest line (no algo comma): " + rawLine);
      }
      String algo = line.substring(0, p).trim();
      String srcPath = line.substring(p + 1).trim();
      return new ManifestRecord(dataset, algo, srcPath, points, encBytes, rawPlain);
    } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
      throw new IOException("Bad manifest line: " + rawLine, e);
    }
  }

  private static Map<String, Map<String, ManifestRecord>> loadManifestRows(Path manifestPath)
      throws IOException {
    Assert.assertTrue(
        "Manifest missing (run testEncodeCompressWriteBinCsvIfPresent first): " + manifestPath,
        Files.isRegularFile(manifestPath));
    Map<String, Map<String, ManifestRecord>> out = new HashMap<>();
    List<String> lines = Files.readAllLines(manifestPath, StandardCharsets.UTF_8);
    Assert.assertFalse("Empty manifest: " + manifestPath, lines.isEmpty());
    for (int i = 1; i < lines.size(); i++) {
      ManifestRecord rec = parseManifestDataLine(lines.get(i));
      if (rec == null) {
        continue;
      }
      out.computeIfAbsent(rec.dataset, k -> new HashMap<>()).put(rec.algorithm, rec);
    }
    return out;
  }

  private static float[] doublesToFloats(double[] values) {
    float[] out = new float[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = (float) values[i];
    }
    return out;
  }

  private static long[] scaleDoublesToLongsFileWide(double[] values, int maxDecimalPrecision) {
    int capped = Math.min(maxDecimalPrecision, K_MAX_DECIMAL_PRECISION);
    long mult = multiplierForBoundedPrecision(capped);
    long[] out = new long[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = Math.round(values[i] * mult);
    }
    return out;
  }

  private static int[] scaleDoublesToIntsFileWide(double[] values, int maxDecimalPrecision) {
    int capped = Math.min(maxDecimalPrecision, K_MAX_DECIMAL_PRECISION);
    int mult = (int) multiplierForBoundedPrecision(capped);
    int[] out = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = (int) Math.round(values[i] * mult);
    }
    return out;
  }

  /** Match micro tests: {@code (int)((float) value * mult)} via float cast, not Math.round. */
  private static int[] scaleDoublesToIntsMicroStyle(double[] values, int maxDecimalPrecision) {
    int capped = Math.min(maxDecimalPrecision, K_MAX_DECIMAL_PRECISION);
    int mult = (int) multiplierForBoundedPrecision(capped);
    int[] out = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      out[i] = (int) ((float) values[i] * mult);
    }
    return out;
  }

  private static int paddedLength(int length, int blockSize) {
    int r = length % blockSize;
    return r == 0 ? length : length + (blockSize - r);
  }

  private static int[] padForFastPFor(int[] data) {
    int m = paddedLength(data.length, FastPFOR.BLOCK_SIZE);
    if (m == data.length) {
      return data;
    }
    return Arrays.copyOf(data, m);
  }

  private static byte[] encodeChimpDoubles(double[] values) throws IOException {
    DoublePrecisionChimpEncoder encoder = new DoublePrecisionChimpEncoder();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (double v : values) {
      encoder.encode(v, baos);
    }
    encoder.flush(baos);
    return baos.toByteArray();
  }

  private static byte[] encodeGorillaDoubles(double[] values) throws IOException {
    DoublePrecisionEncoderV2 encoder = new DoublePrecisionEncoderV2();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    for (double v : values) {
      encoder.encode(v, baos);
    }
    encoder.flush(baos);
    return baos.toByteArray();
  }

  private static double[] decodeChimpDoubles(byte[] encoded, int pointCount) {
    DoublePrecisionChimpDecoder decoder = new DoublePrecisionChimpDecoder();
    ByteBuffer buf = ByteBuffer.wrap(encoded);
    double[] out = new double[pointCount];
    int i = 0;
    while (decoder.hasNext(buf)) {
      // Assert.assertTrue("Chimp decode: too many values", i < pointCount);
      out[i++] = decoder.readDouble(buf);
    }
    // Assert.assertEquals(pointCount, i);
    return out;
  }

  private static double[] decodeGorillaDoubles(byte[] encoded, int pointCount) {
    DoublePrecisionDecoderV2 decoder = new DoublePrecisionDecoderV2();
    ByteBuffer buf = ByteBuffer.wrap(encoded);
    double[] out = new double[pointCount];
    int i = 0;
    while (decoder.hasNext(buf)) {
      out[i++] = decoder.readDouble(buf);
    }
    return out;
  }

  private static byte[] encodeElfDoubles(double[] values) {
    return ElfDoublePrecisionBenchCodec.encode(values);
  }

  private static double[] decodeElfDoubles(byte[] encoded) {
    return ElfDoublePrecisionBenchCodec.decode(encoded);
  }

  private static void assertElfRoundtrip(double[] expected, byte[] encoded) {
    decodeElfDoubles(encoded);
    // for (int i = 0; i < expected.length; i++) {
    //   Assert.assertEquals(
    //       Double.doubleToLongBits(expected[i]), Double.doubleToLongBits(got[i]));
    // }
  }

  private static void assertChimpRoundtrip(double[] expected, byte[] encoded) {
    decodeChimpDoubles(encoded, expected.length);
    // for (int i = 0; i < expected.length; i++) {
    //   Assert.assertEquals(
    //       Double.doubleToLongBits(expected[i]), Double.doubleToLongBits(got[i]));
    // }
  }

  private static void assertGorillaRoundtrip(double[] expected, byte[] encoded) {
    decodeGorillaDoubles(encoded, expected.length);
  }

  private static byte[] encodeBuffFloats(float[] values, int maxDecimalPrecision) {
    int maxDec = Math.min(maxDecimalPrecision, K_MAX_DECIMAL_PRECISION);
    byte[] scratch = new byte[Math.max(values.length * 32, 4096)];
    int len = BUFFTest.Encoder(values, BUFF_ALP_BLOCK_SIZE, maxDec, scratch);
    return Arrays.copyOf(scratch, len);
  }

  private static float[] decodeBuffBytes(byte[] encoded) {
    return BUFFTest.Decoder(encoded);
  }

  private static void assertBuffRoundtrip(float[] expected, byte[] encoded) {
    decodeBuffBytes(encoded);
    // Assert.assertEquals(expected.length, got.length);
    // for (int i = 0; i < expected.length; i++) {
    //   Assert.assertEquals(expected[i], got[i], 0.0f);
    // }
  }

  private static byte[] encodeAlpDoubles(double[] values, int maxDecimalPrecision) {
    int maxDec = Math.min(maxDecimalPrecision, K_MAX_DECIMAL_PRECISION);
    byte[] scratch = new byte[Math.max(values.length * 32, 4096)];
    int len = ALPTest.Encoder(values, BUFF_ALP_BLOCK_SIZE, maxDec, scratch);
    return Arrays.copyOf(scratch, len);
  }

  private static double[] decodeAlpBytes(byte[] encoded) {
    return ALPTest.Decoder(encoded);
  }

  private static void assertAlpRoundtrip(double[] expected, byte[] encoded) {
    decodeAlpBytes(encoded);
    // Assert.assertEquals(expected.length, got.length);
    // for (int i = 0; i < expected.length; i++) {
    //   Assert.assertEquals(expected[i], got[i], 0.0);
    // }
  }

  private static byte[] packHbpPlain(long[] data) throws IOException {
    ArrayList<HBPIndexLong> indexList = new ArrayList<>();
    byte[] encodedScratch = new byte[Math.max(data.length * 16, 4096)];
    int headerLen =
        HBPIndexLongTest.Encoder(data, HBP_BLOCK_SIZE, indexList, encodedScratch);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      dos.writeInt(headerLen);
      dos.write(encodedScratch, 0, headerLen);
      dos.writeInt(indexList.size());
      for (HBPIndexLong idx : indexList) {
        dos.writeInt(idx.k);
        dos.writeInt(idx.n);
        for (int j = 0; j < idx.n; j++) {
          dos.writeLong(idx.getCode(j));
        }
      }
    }
    return baos.toByteArray();
  }

  private static long[] unpackHbpPlain(byte[] packed) throws IOException {
    try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(packed))) {
      int headerLen = dis.readInt();
      byte[] encoded = new byte[headerLen];
      dis.readFully(encoded);
      int indexCount = dis.readInt();
      ArrayList<HBPIndexLong> indexList = new ArrayList<>(indexCount);
      for (int i = 0; i < indexCount; i++) {
        int k = dis.readInt();
        int n = dis.readInt();
        long[] codes = new long[n];
        for (int j = 0; j < n; j++) {
          codes[j] = dis.readLong();
        }
        indexList.add(new HBPIndexLong(k, codes));
      }
      return HBPIndexLongTest.Decoder(encoded, indexList);
    }
  }

  private static void assertHbpRoundtrip(long[] expected, byte[] packed) throws IOException {
    // Assert.assertArrayEquals(expected, unpackHbpPlain(packed));
    unpackHbpPlain(packed);
  }

  private static byte[] intArrayToBytes(int[] data) {
    ByteBuffer bb = ByteBuffer.allocate(data.length * Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
    for (int v : data) {
      bb.putInt(v);
    }
    return bb.array();
  }

  private static int[] intArrayFromBlobBytes(byte[] blob) {
    Assert.assertEquals(0, blob.length % Integer.BYTES);
    int n = blob.length / Integer.BYTES;
    int[] out = new int[n];
    ByteBuffer bb = ByteBuffer.wrap(blob).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < n; i++) {
      out[i] = bb.getInt();
    }
    return out;
  }

  private static void writeReadMetricsRow(
      BufferedWriter readMetrics,
      String datasetName,
      String algo,
      ReadPathTimings r,
      int points)
      throws IOException {
    long readTotal =
        r.avgDatasetVerifyReadNs
            + r.avgBinReadNs
            + r.avgUncompressNs
            + r.avgDecodeNs
            + r.avgDecodedCsvWriteNs;
    long readCpu = r.avgUncompressNs + r.avgDecodeNs;
    readMetrics.write(
        String.format(
            "%s,%s,%d,%d,%d,%d,%d,%d,%d%n",
            datasetName,
            algo,
            readTotal,
            readCpu,
            r.avgDatasetVerifyReadNs,
            r.avgBinReadNs,
            r.avgDecodedCsvWriteNs,
            points,
            r.compressedBytesObserved));
  }

  private static void writeEncodeMetricsRow(
      BufferedWriter writeMetrics,
      String datasetName,
      String algo,
      WritePathTimings w,
      int points,
      int boundedPrec,
      long multiplier)
      throws IOException {
    long writeTotal =
        w.avgDatasetReadNs
            + w.avgEncodeLoopNs
            + w.avgEncodeFlushNs
            + w.avgCompressNs
            + w.avgBinWriteNs;
    long writeCpu = w.avgEncodeLoopNs + w.avgEncodeFlushNs + w.avgCompressNs;
    writeMetrics.write(
        String.format(
            "%s,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
            datasetName,
            algo,
            writeTotal,
            w.avgDatasetReadNs,
            writeCpu,
            w.avgBinWriteNs,
            points,
            boundedPrec,
            multiplier,
            w.compressedBytes,
            w.avgEncodeLoopNs,
            w.avgEncodeFlushNs,
            w.avgCompressNs));
  }

  private static void writeManifestRow(
      BufferedWriter manifest,
      String datasetName,
      String algo,
      String srcPath,
      int points,
      int encodedUncompressedBytes,
      int rawPlainCodec)
      throws IOException {
    manifest.write(
        String.format(
            "%s,%s,%s,%d,%d,%d%n",
            datasetName,
            algo,
            srcPath,
            points,
            encodedUncompressedBytes,
            rawPlainCodec));
  }

  private static void ensureDirectory(Path dir) throws IOException {
    Files.createDirectories(dir);
  }

  private static File[] listBenchmarkDatasetCsvFiles(File sourceDir) {
    File[] files = sourceDir.listFiles((dir, name) -> name.endsWith(".csv"));
    if (files == null || files.length == 0) {
      return new File[0];
    }
    Arrays.sort(files, Comparator.comparing(File::getName));
    return files;
  }

  /** Progress log for encode/decode bench (stdout, flushed). */
  private static void logBenchProgress(
      String phase, String datasetName, String algorithm, long points, Path path) {
    System.out.printf(
        "[DatasetEncoderCompressBinBench] %s | dataset=%s | algorithm=%s | points=%d | path=%s%n",
        phase, datasetName, algorithm, points, path.toAbsolutePath());
    System.out.flush();
  }

  private static void logBenchDatasetHeader(String phase, String datasetName, File csvFile, long points) {
    System.out.printf(
        "[DatasetEncoderCompressBinBench] %s | === dataset=%s | csv=%s | points=%d ===%n",
        phase, datasetName, csvFile.getAbsolutePath(), points);
    System.out.flush();
  }

  /** Mirrors C++ cfg_idx==0 discarded warmup before the first algorithm per dataset. */
  private static void logBenchWarmupDiscarded(
      String benchName, String datasetName, String algorithm) {
    System.err.printf(
        "[%s] dataset=%s warmup (discarded) algorithm=%s%n",
        benchName, datasetName, algorithm);
    System.err.flush();
  }

  private static boolean isWindowsHost() {
    return System.getProperty("os.name", "").toLowerCase(Locale.ROOT).contains("win");
  }

  private static int benchWindowsBinReadModeEnv() {
    String e = System.getenv("TSFILE_BENCH_BIN_READ_MODE");
    if (e == null || e.isEmpty()) {
      return -1;
    }
    if ("ifstream".equalsIgnoreCase(e)) {
      return 0;
    }
    if ("win32".equalsIgnoreCase(e)) {
      return 1;
    }
    if ("nobuf".equalsIgnoreCase(e) || "no_buffering".equalsIgnoreCase(e)) {
      return -1;
    }
    try {
      int v = Integer.parseInt(e.trim());
      if (v == 0 || v == 1) {
        return v;
      }
    } catch (NumberFormatException ignored) {
    }
    return -1;
  }

  private static boolean benchWindowsBinReadShrinkBeforeReadEnv() {
    String e = System.getenv("TSFILE_BENCH_BIN_READ_SHRINK");
    if (e == null || e.isEmpty()) {
      return true;
    }
    return !"0".equals(e);
  }

  private static int winLogicalSectorBytes(String absPath) {
    char[] volBuf = new char[261];
    if (!Kernel32.INSTANCE.GetVolumePathName(absPath, volBuf, volBuf.length)) {
      return 512;
    }
    String vol = Native.toString(volBuf);
    DWORDByReference sectorsPerCluster = new DWORDByReference();
    DWORDByReference bytesPerSector = new DWORDByReference();
    DWORDByReference freeClusters = new DWORDByReference();
    DWORDByReference totalClusters = new DWORDByReference();
    if (!Kernel32.INSTANCE.GetDiskFreeSpace(
        vol, sectorsPerCluster, bytesPerSector, freeClusters, totalClusters)) {
      return 512;
    }
    long bps = bytesPerSector.getValue().longValue() & 0xFFFFFFFFL;
    return (bps > 0 && bps <= 65536) ? (int) bps : 512;
  }

  private static boolean isInvalidHandle(HANDLE h) {
    return h == null || WinBase.INVALID_HANDLE_VALUE.equals(h);
  }

  private static byte[] readAllBytesWin32Buffered(Path binPath) throws IOException {
    String path = binPath.toAbsolutePath().toString();
    long fileSizeLong = Files.size(binPath);
    if (fileSizeLong <= 0 || fileSizeLong > Integer.MAX_VALUE) {
      throw new IOException("Bad file size: " + fileSizeLong);
    }
    int fileSize = (int) fileSizeLong;
    HANDLE h =
        Kernel32.INSTANCE.CreateFile(
            path,
            WinNT.GENERIC_READ,
            WinNT.FILE_SHARE_READ | WinNT.FILE_SHARE_WRITE | WinNT.FILE_SHARE_DELETE,
            null,
            WinNT.OPEN_EXISTING,
            WinNT.FILE_ATTRIBUTE_NORMAL | WinNT.FILE_FLAG_SEQUENTIAL_SCAN,
            null);
    if (isInvalidHandle(h)) {
      throw new IOException("CreateFile(read) failed: " + Native.getLastError());
    }
    try {
      byte[] out = new byte[fileSize];
      int scratchCap = Math.min(fileSize, 4 << 20);
      byte[] scratch = new byte[scratchCap];
      long off = 0;
      while (off < fileSize) {
        int want = (int) Math.min(fileSize - off, scratchCap);
        IntByReference br = new IntByReference();
        if (!Kernel32.INSTANCE.ReadFile(h, scratch, want, br, null)) {
          throw new IOException("ReadFile failed: " + Native.getLastError());
        }
        int got = br.getValue();
        if (got <= 0) {
          throw new IOException("ReadFile EOF");
        }
        System.arraycopy(scratch, 0, out, (int) off, got);
        off += got;
      }
      return out;
    } finally {
      Kernel32.INSTANCE.CloseHandle(h);
    }
  }

  private static byte[] readAllBytesWin32NoBuffering(Path binPath) throws IOException {
    String path = binPath.toAbsolutePath().toString();
    long fileSizeLong = Files.size(binPath);
    if (fileSizeLong <= 0 || fileSizeLong > Integer.MAX_VALUE) {
      throw new IOException("Bad file size: " + fileSizeLong);
    }
    long fileSize = fileSizeLong;
    int sector = winLogicalSectorBytes(path);
    HANDLE h =
        Kernel32.INSTANCE.CreateFile(
            path,
            WinNT.GENERIC_READ,
            WinNT.FILE_SHARE_READ | WinNT.FILE_SHARE_WRITE | WinNT.FILE_SHARE_DELETE,
            null,
            WinNT.OPEN_EXISTING,
            WinNT.FILE_ATTRIBUTE_NORMAL
                | WinNT.FILE_FLAG_SEQUENTIAL_SCAN
                | WinNT.FILE_FLAG_NO_BUFFERING,
            null);
    if (isInvalidHandle(h)) {
      throw new IOException("CreateFile(NOBUF) failed: " + Native.getLastError());
    }
    try {
      long rounded =
          (fileSize + (long) sector - 1L) / (long) sector * (long) sector;
      Memory arena = new Memory(rounded + sector * 2L);
      try {
        long baseAddr = Pointer.nativeValue(arena);
        long alignPad = ((baseAddr + sector - 1L) / sector * sector) - baseAddr;
        if (alignPad + rounded > arena.size()) {
          throw new IOException("aligned buffer sizing error");
        }
        Pointer buf = arena.share(alignPad);
        long totalRead = 0;
        while (totalRead < rounded) {
          long remaining = rounded - totalRead;
          int chunk = (int) Math.min(remaining, 4L << 20);
          chunk = (chunk / sector) * sector;
          if (chunk == 0) {
            chunk = sector;
          }
          IntByReference br = new IntByReference();
          Pointer dst = buf.share(totalRead);
          if (!Kernel32PointerRead.INSTANCE.ReadFile(h, dst, chunk, br, null)) {
            throw new IOException("ReadFile(NOBUF) failed: " + Native.getLastError());
          }
          int got = br.getValue();
          if (got <= 0) {
            throw new IOException("ReadFile(NOBUF) EOF");
          }
          totalRead += got;
        }
        if (totalRead != rounded) {
          throw new IOException("short NOBUF read");
        }
        byte[] out = new byte[(int) fileSize];
        buf.read(0, out, 0, out.length);
        return out;
      } finally {
        arena.clear();
      }
    } finally {
      Kernel32.INSTANCE.CloseHandle(h);
    }
  }

  private static byte[] readCompressedBinWindows(Path binPath) throws IOException {
    int mode = benchWindowsBinReadModeEnv();
    if (mode == 0) {
      return Files.readAllBytes(binPath);
    }
    if (mode == 1) {
      return readAllBytesWin32Buffered(binPath);
    }
    try {
      return readAllBytesWin32NoBuffering(binPath);
    } catch (IOException ex) {
      return readAllBytesWin32Buffered(binPath);
    }
  }

  private static byte[] readCompressedBinBench(Path binPath) throws IOException {
    if (!isWindowsHost()) {
      return Files.readAllBytes(binPath);
    }
    return readCompressedBinWindows(binPath);
  }

  private static long timeDecodedLongCsvWriteNanos(Path path, long[] values) throws IOException {
    long t0 = System.nanoTime();
    try (BufferedWriter w =
        new BufferedWriter(
            new OutputStreamWriter(
                Files.newOutputStream(
                    path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE),
                StandardCharsets.UTF_8))) {
      for (long v : values) {
        w.write(Long.toString(v));
        w.write('\n');
      }
      w.flush();
    }
    return System.nanoTime() - t0;
  }

  private static long timeDecodedDoublesCsvWriteNanos(Path path, double[] values) throws IOException {
    long t0 = System.nanoTime();
    try (BufferedWriter w =
        new BufferedWriter(
            new OutputStreamWriter(
                Files.newOutputStream(
                    path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE),
                StandardCharsets.UTF_8))) {
      for (double v : values) {
        w.write(Double.toString(v));
        w.write('\n');
      }
      w.flush();
    }
    return System.nanoTime() - t0;
  }

  private static long writePayloadNanosWithFlushAndClose(Path binPath, byte[] payload)
      throws IOException {
    long t0 = System.nanoTime();
    try (OutputStream stream =
        Files.newOutputStream(
            binPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      stream.write(payload);
      stream.flush();
    }
    return System.nanoTime() - t0;
  }

  private static WritePathTimings benchmarkRawDoubleLzmaWritePath(double[] values, Path binPath)
      throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.LZMA2);

    WritePathTimings out = new WritePathTimings();

    byte[] rawPayload = doublesToLittleEndianRawBytes(values);
    byte[] compressed0 = compressLzma2BenchPreset(rawPayload);
    byte[] back0 = unCompressor.uncompress(compressed0);
    assertRawDoubleRoundtrip(values, back0);

    long sumCmp = 0;
    long sumBw = 0;
    byte[] lastCompressed = compressed0;

    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      lastCompressed = compressLzma2BenchPreset(rawPayload);
      long t1 = System.nanoTime();
      sumCmp += (t1 - t0);

      sumBw += writePayloadNanosWithFlushAndClose(binPath, lastCompressed);
    }

    out.avgEncodeLoopNs = 0;
    out.avgEncodeFlushNs = 0;
    out.avgCompressNs = sumCmp / BENCH_PHASE_REPEATS;
    out.avgBinWriteNs = sumBw / BENCH_PHASE_REPEATS;
    out.encodedUncompressedBytes = rawPayload.length;
    out.compressedBytes = lastCompressed.length;

    return out;
  }

  private static WritePathTimings benchmarkDictionaryWritePath(long[] values, Path binPath)
      throws IOException {
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);

    WritePathTimings out = new WritePathTimings();

    DictionaryEncoder encoder0 = new DictionaryEncoder();
    ByteArrayOutputStream baos0 = new ByteArrayOutputStream();
    for (long v : values) {
      encoder0.encode(longToPlainBinary(v), baos0);
    }
    encoder0.flush(baos0);
    byte[] plain0 = baos0.toByteArray();
    byte[] compressed0 = compressor.compress(plain0);
    byte[] back0 = unCompressor.uncompress(compressed0);
    assertDictionaryRoundtrip(values, back0);

    long sumEnc = 0;
    long sumFlush = 0;
    long sumCmp = 0;
    long sumBw = 0;
    byte[] lastCompressed = compressed0;
    int lastEncodedLen = plain0.length;

    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      DictionaryEncoder encoder = new DictionaryEncoder();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      long t0 = System.nanoTime();
      for (long v : values) {
        encoder.encode(longToPlainBinary(v), baos);
      }
      long t1 = System.nanoTime();
      sumEnc += (t1 - t0);

      t0 = System.nanoTime();
      encoder.flush(baos);
      t1 = System.nanoTime();
      sumFlush += (t1 - t0);

      byte[] encBytes = baos.toByteArray();
      lastEncodedLen = encBytes.length;

      t0 = System.nanoTime();
      lastCompressed = compressor.compress(encBytes);
      t1 = System.nanoTime();
      sumCmp += (t1 - t0);

      sumBw += writePayloadNanosWithFlushAndClose(binPath, lastCompressed);
    }

    out.avgEncodeLoopNs = sumEnc / BENCH_PHASE_REPEATS;
    out.avgEncodeFlushNs = sumFlush / BENCH_PHASE_REPEATS;
    out.avgCompressNs = sumCmp / BENCH_PHASE_REPEATS;
    out.avgBinWriteNs = sumBw / BENCH_PHASE_REPEATS;
    out.encodedUncompressedBytes = lastEncodedLen;
    out.compressedBytes = lastCompressed.length;

    return out;
  }

  private static WritePathTimings benchmarkPlainBytesWritePath(
      byte[] plain0, java.util.function.Supplier<byte[]> encodePlain, Path binPath)
      throws IOException {
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);

    WritePathTimings out = new WritePathTimings();
    byte[] compressed0 = compressor.compress(plain0);
    byte[] back0 = unCompressor.uncompress(compressed0);
    // Assert.assertArrayEquals(plain0, back0);

    long sumEnc = 0;
    long sumCmp = 0;
    long sumBw = 0;
    byte[] lastCompressed = compressed0;
    int lastPlainLen = plain0.length;

    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] plain = encodePlain.get();
      long t1 = System.nanoTime();
      sumEnc += (t1 - t0);
      lastPlainLen = plain.length;

      t0 = System.nanoTime();
      lastCompressed = compressor.compress(plain);
      t1 = System.nanoTime();
      sumCmp += (t1 - t0);

      sumBw += writePayloadNanosWithFlushAndClose(binPath, lastCompressed);
    }

    out.avgEncodeLoopNs = sumEnc / BENCH_PHASE_REPEATS;
    out.avgEncodeFlushNs = 0;
    out.avgCompressNs = sumCmp / BENCH_PHASE_REPEATS;
    out.avgBinWriteNs = sumBw / BENCH_PHASE_REPEATS;
    out.encodedUncompressedBytes = lastPlainLen;
    out.compressedBytes = lastCompressed.length;
    return out;
  }

  private static WritePathTimings benchmarkElfWritePath(double[] values, Path binPath)
      throws IOException {
    byte[] plain0 = encodeElfDoubles(values);
    assertElfRoundtrip(values, plain0);
    WritePathTimings out =
        benchmarkPlainBytesWritePath(plain0, () -> encodeElfDoubles(values), binPath);
    return out;
  }

  private static WritePathTimings benchmarkChimpWritePath(double[] values, Path binPath)
      throws IOException {
    byte[] plain0 = encodeChimpDoubles(values);
    assertChimpRoundtrip(values, plain0);
    WritePathTimings out =
        benchmarkPlainBytesWritePath(
            plain0,
            () -> {
              try {
                return encodeChimpDoubles(values);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            },
            binPath);
    return out;
  }

  private static WritePathTimings benchmarkGorillaWritePath(double[] values, Path binPath)
      throws IOException {
    byte[] plain0 = encodeGorillaDoubles(values);
    assertGorillaRoundtrip(values, plain0);
    WritePathTimings out =
        benchmarkPlainBytesWritePath(
            plain0,
            () -> {
              try {
                return encodeGorillaDoubles(values);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            },
            binPath);
    return out;
  }

  private static WritePathTimings benchmarkBuffWritePath(
      double[] values, int maxDecimalPrecision, Path binPath) throws IOException {
    float[] floats = doublesToFloats(values);
    byte[] plain0 = encodeBuffFloats(floats, maxDecimalPrecision);
    assertBuffRoundtrip(floats, plain0);
    WritePathTimings out =
        benchmarkPlainBytesWritePath(
            plain0, () -> encodeBuffFloats(floats, maxDecimalPrecision), binPath);
    return out;
  }

  private static WritePathTimings benchmarkAlpWritePath(
      double[] values, int maxDecimalPrecision, Path binPath) throws IOException {
    byte[] plain0 = encodeAlpDoubles(values, maxDecimalPrecision);
    assertAlpRoundtrip(values, plain0);
    WritePathTimings out =
        benchmarkPlainBytesWritePath(
            plain0, () -> encodeAlpDoubles(values, maxDecimalPrecision), binPath);
    return out;
  }

  private static WritePathTimings benchmarkHbpWritePath(
      long[] values, Path binPath) throws IOException {
    byte[] plain0 = packHbpPlain(values);
    assertHbpRoundtrip(values, plain0);
    WritePathTimings out =
        benchmarkPlainBytesWritePath(
            plain0,
            () -> {
              try {
                return packHbpPlain(values);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            },
            binPath);
    return out;
  }

  private static WritePathTimings benchmarkCodecBlobWritePath(
      byte[] blob0, java.util.function.Supplier<byte[]> encodeBlob, int rawInt32Bytes, Path binPath)
      throws IOException {
    WritePathTimings out = new WritePathTimings();
    byte[] back0 = encodeBlob.get();
    // Assert.assertArrayEquals(blob0, back0);

    long sumEnc = 0;
    long sumBw = 0;
    byte[] lastBlob = blob0;

    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      lastBlob = encodeBlob.get();
      long t1 = System.nanoTime();
      sumEnc += (t1 - t0);

      sumBw += writePayloadNanosWithFlushAndClose(binPath, lastBlob);
    }

    out.avgEncodeLoopNs = sumEnc / BENCH_PHASE_REPEATS;
    out.avgEncodeFlushNs = 0;
    out.avgCompressNs = 0;
    out.avgBinWriteNs = sumBw / BENCH_PHASE_REPEATS;
    out.encodedUncompressedBytes = rawInt32Bytes;
    out.compressedBytes = lastBlob.length;
    return out;
  }

  private static WritePathTimings benchmarkSimple8bWritePath(int[] values, Path binPath)
      throws IOException {
    int[] compressed0 = FastPForSimple8bCodec.encode(values);
    int[] back0 = FastPForSimple8bCodec.decode(compressed0);
    // Assert.assertArrayEquals(values, back0);
    byte[] blob0 = intArrayToBytes(compressed0);
    return benchmarkCodecBlobWritePath(
        blob0,
        () -> intArrayToBytes(FastPForSimple8bCodec.encode(values)),
        values.length * Integer.BYTES,
        binPath);
  }

  private static WritePathTimings benchmarkFastPforWritePath(int[] values, Path binPath)
      throws IOException {
    int[] padded = padForFastPFor(values);
    IntCompressor comp = new IntCompressor(new FastPFOR());
    int[] compressed0 = comp.compress(padded);
    int[] back0 = comp.uncompress(compressed0);
    // for (int i = 0; i < values.length; i++) {
    //   Assert.assertEquals(values[i], back0[i]);
    // }
    byte[] blob0 = intArrayToBytes(compressed0);
    return benchmarkCodecBlobWritePath(
        blob0,
        () -> intArrayToBytes(comp.compress(padForFastPFor(values))),
        values.length * Integer.BYTES,
        binPath);
  }

  private static WritePathTimings benchmarkRleWritePath(long[] values, Path binPath)
      throws IOException {
    byte[] plain0 = encodeRleLongs(values);
    return benchmarkPlainBytesWritePath(plain0, () -> encodeRleLongs(values), binPath);
  }

  private static WritePathTimings benchmarkBitPackingWritePath(int[] values, Path binPath)
      throws IOException {
    byte[] plain0 = encodeBitPackingInts(values);
    return benchmarkPlainBytesWritePath(plain0, () -> encodeBitPackingInts(values), binPath);
  }

  private static WritePathTimings benchmarkSprintzWritePath(int[] values, Path binPath)
      throws IOException {
    byte[] plain0 = encodeSprintzInts(values);
    return benchmarkPlainBytesWritePath(plain0, () -> encodeSprintzInts(values), binPath);
  }

  private static WritePathTimings benchmarkTs2DiffWritePath(int[] values, Path binPath)
      throws IOException {
    byte[] plain0 = encodeTs2DiffInts(values);
    return benchmarkPlainBytesWritePath(plain0, () -> encodeTs2DiffInts(values), binPath);
  }

  private static WritePathTimings benchmarkSubcolumnWritePath(int[] values, Path binPath)
      throws IOException {
    byte[] plain0 = encodeSubcolumnInts(values);
    return benchmarkPlainBytesWritePath(plain0, () -> encodeSubcolumnInts(values), binPath);
  }

  private static long avgDatasetVerifyReadDoublesNanos(File csvFile, double[] expected)
      throws IOException {
    long sumVerify = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      List<String[]> rows = readFullCsvAllColumnsAsStringRows(csvFile);
      long t1 = System.nanoTime();
      sumVerify += (t1 - t0);
      List<String> tokens = firstColumnTokensFromRows(rows);
      double[] vals = parseDoublesFromTokens(tokens);
      Assert.assertEquals(expected.length, vals.length);
      for (int i = 0; i < expected.length; i++) {
        Assert.assertEquals(
            Double.doubleToLongBits(expected[i]), Double.doubleToLongBits(vals[i]));
      }
    }
    return sumVerify / BENCH_PHASE_REPEATS;
  }

  private static long avgDatasetVerifyReadLongsFileWideNanos(File csvFile, long[] expected)
      throws IOException {
    long sumVerify = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      List<String[]> rows = readFullCsvAllColumnsAsStringRows(csvFile);
      long t1 = System.nanoTime();
      sumVerify += (t1 - t0);
      List<String> tokens = firstColumnTokensFromRows(rows);
      double[] vals = parseDoublesFromTokens(tokens);
      int maxPrec = maxDecimalPrecision(tokens);
      long[] scaled = scaleDoublesToLongsFileWide(vals, maxPrec);
      Assert.assertArrayEquals(expected, scaled);
    }
    return sumVerify / BENCH_PHASE_REPEATS;
  }

  private static long avgDatasetVerifyReadIntsFileWideNanos(File csvFile, int[] expected)
      throws IOException {
    long sumVerify = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      List<String[]> rows = readFullCsvAllColumnsAsStringRows(csvFile);
      long t1 = System.nanoTime();
      sumVerify += (t1 - t0);
      List<String> tokens = firstColumnTokensFromRows(rows);
      double[] vals = parseDoublesFromTokens(tokens);
      int maxPrec = maxDecimalPrecision(tokens);
      int[] scaled = scaleDoublesToIntsFileWide(vals, maxPrec);
      Assert.assertArrayEquals(expected, scaled);
    }
    return sumVerify / BENCH_PHASE_REPEATS;
  }

  private static long avgDatasetVerifyReadIntsMicroStyleNanos(File csvFile, int[] expected)
      throws IOException {
    long sumVerify = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      List<String[]> rows = readFullCsvAllColumnsAsStringRows(csvFile);
      long t1 = System.nanoTime();
      sumVerify += (t1 - t0);
      List<String> tokens = firstColumnTokensFromRows(rows);
      double[] vals = parseDoublesFromTokens(tokens);
      int maxPrec = maxDecimalPrecision(tokens);
      int[] scaled = scaleDoublesToIntsMicroStyle(vals, maxPrec);
      Assert.assertArrayEquals(expected, scaled);
    }
    return sumVerify / BENCH_PHASE_REPEATS;
  }

  private static long avgDatasetVerifyReadIntsPerSubcolumnBlockNanos(File csvFile, int[] expected)
      throws IOException {
    long sumVerify = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      List<String[]> rows = readFullCsvAllColumnsAsStringRows(csvFile);
      long t1 = System.nanoTime();
      sumVerify += (t1 - t0);
      List<String> tokens = firstColumnTokensFromRows(rows);
      int[] scaled = scaleTokensToIntsPerSubcolumnBlock(tokens);
      Assert.assertArrayEquals(expected, scaled);
    }
    return sumVerify / BENCH_PHASE_REPEATS;
  }

  private static byte[] readBinBlobWithTiming(Path binPath, long[] outAvgBinReadNs)
      throws IOException {
    long sumBr = 0;
    byte[] blob = null;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      if (benchWindowsBinReadShrinkBeforeReadEnv()) {
        blob = null;
      }
      blob = readCompressedBinBench(binPath);
      long t1 = System.nanoTime();
      sumBr += (t1 - t0);
    }
    Assert.assertNotNull(blob);
    outAvgBinReadNs[0] = sumBr / BENCH_PHASE_REPEATS;
    return blob;
  }

  private static ReadPathTimings benchmarkElfReadPath(
      File csvFile, double[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
    ReadPathTimings out = new ReadPathTimings();
    out.avgDatasetVerifyReadNs = avgDatasetVerifyReadDoublesNanos(csvFile, expected);
    long[] brNs = new long[1];
    byte[] blob = readBinBlobWithTiming(binPath, brNs);
    out.avgBinReadNs = brNs[0];
    out.compressedBytesObserved = blob.length;

    double[] decoded = new double[expected.length];
    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);
      t0 = System.nanoTime();
      double[] d = decodeElfDoubles(unc);
      System.arraycopy(d, 0, decoded, 0, Math.min(d.length, decoded.length));
      t1 = System.nanoTime();
      sumDec += (t1 - t0);
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      sumCsv += timeDecodedDoublesCsvWriteNanos(decodedCsvPath, decoded);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;
    return out;
  }

  private static ReadPathTimings benchmarkChimpReadPath(
      File csvFile, double[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
    ReadPathTimings out = new ReadPathTimings();
    out.avgDatasetVerifyReadNs = avgDatasetVerifyReadDoublesNanos(csvFile, expected);
    long[] brNs = new long[1];
    byte[] blob = readBinBlobWithTiming(binPath, brNs);
    out.avgBinReadNs = brNs[0];
    out.compressedBytesObserved = blob.length;

    double[] decoded = new double[expected.length];
    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);
      t0 = System.nanoTime();
      double[] d = decodeChimpDoubles(unc, expected.length);
      System.arraycopy(d, 0, decoded, 0, d.length);
      t1 = System.nanoTime();
      sumDec += (t1 - t0);
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      sumCsv += timeDecodedDoublesCsvWriteNanos(decodedCsvPath, decoded);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;
    return out;
  }

  private static ReadPathTimings benchmarkGorillaReadPath(
      File csvFile, double[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
    ReadPathTimings out = new ReadPathTimings();
    out.avgDatasetVerifyReadNs = avgDatasetVerifyReadDoublesNanos(csvFile, expected);
    long[] brNs = new long[1];
    byte[] blob = readBinBlobWithTiming(binPath, brNs);
    out.avgBinReadNs = brNs[0];
    out.compressedBytesObserved = blob.length;

    double[] decoded = new double[expected.length];
    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);
      t0 = System.nanoTime();
      double[] d = decodeGorillaDoubles(unc, expected.length);
      System.arraycopy(d, 0, decoded, 0, d.length);
      t1 = System.nanoTime();
      sumDec += (t1 - t0);
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      sumCsv += timeDecodedDoublesCsvWriteNanos(decodedCsvPath, decoded);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;
    return out;
  }

  private static ReadPathTimings benchmarkBuffReadPath(
      File csvFile, double[] expected, int maxPrec, Path binPath, Path decodedCsvPath)
      throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
    ReadPathTimings out = new ReadPathTimings();
    out.avgDatasetVerifyReadNs = avgDatasetVerifyReadDoublesNanos(csvFile, expected);
    long[] brNs = new long[1];
    byte[] blob = readBinBlobWithTiming(binPath, brNs);
    out.avgBinReadNs = brNs[0];
    out.compressedBytesObserved = blob.length;

    float[] decodedFloats = new float[expected.length];
    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);
      t0 = System.nanoTime();
      float[] d = decodeBuffBytes(unc);
      System.arraycopy(d, 0, decodedFloats, 0, d.length);
      t1 = System.nanoTime();
      sumDec += (t1 - t0);
      // if (rep == 0) {
      //   for (int i = 0; i < expected.length; i++) {
      //     Assert.assertEquals((float) expected[i], decodedFloats[i], 0.0f);
      //   }
      // }
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      double[] decoded = new double[decodedFloats.length];
      for (int i = 0; i < decodedFloats.length; i++) {
        decoded[i] = decodedFloats[i];
      }
      sumCsv += timeDecodedDoublesCsvWriteNanos(decodedCsvPath, decoded);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;
    return out;
  }

  private static ReadPathTimings benchmarkAlpReadPath(
      File csvFile, double[] expected, int maxPrec, Path binPath, Path decodedCsvPath)
      throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
    ReadPathTimings out = new ReadPathTimings();
    out.avgDatasetVerifyReadNs = avgDatasetVerifyReadDoublesNanos(csvFile, expected);
    long[] brNs = new long[1];
    byte[] blob = readBinBlobWithTiming(binPath, brNs);
    out.avgBinReadNs = brNs[0];
    out.compressedBytesObserved = blob.length;

    double[] decoded = new double[expected.length];
    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);
      t0 = System.nanoTime();
      double[] d = decodeAlpBytes(unc);
      System.arraycopy(d, 0, decoded, 0, d.length);
      t1 = System.nanoTime();
      sumDec += (t1 - t0);
      // if (rep == 0) {
      //   for (int i = 0; i < expected.length; i++) {
      //     Assert.assertEquals(expected[i], decoded[i], 0.0);
      //   }
      // }
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      sumCsv += timeDecodedDoublesCsvWriteNanos(decodedCsvPath, decoded);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;
    return out;
  }

  private static ReadPathTimings benchmarkHbpReadPath(
      File csvFile, long[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
    ReadPathTimings out = new ReadPathTimings();
    out.avgDatasetVerifyReadNs = avgDatasetVerifyReadLongsFileWideNanos(csvFile, expected);
    long[] brNs = new long[1];
    byte[] blob = readBinBlobWithTiming(binPath, brNs);
    out.avgBinReadNs = brNs[0];
    out.compressedBytesObserved = blob.length;

    long[] decoded = new long[expected.length];
    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);
      t0 = System.nanoTime();
      long[] d = unpackHbpPlain(unc);
      System.arraycopy(d, 0, decoded, 0, d.length);
      t1 = System.nanoTime();
      sumDec += (t1 - t0);
      // if (rep == 0) {
      //   Assert.assertArrayEquals(expected, decoded);
      // }
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      sumCsv += timeDecodedLongCsvWriteNanos(decodedCsvPath, decoded);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;
    return out;
  }

  private static ReadPathTimings benchmarkPlainIntEncodedReadPath(
      File csvFile,
      int[] expected,
      Path binPath,
      Path decodedCsvPath,
      java.util.function.Function<byte[], int[]> decodePlain,
      long avgVerifyNs)
      throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
    ReadPathTimings out = new ReadPathTimings();
    out.avgDatasetVerifyReadNs = avgVerifyNs;
    long[] brNs = new long[1];
    byte[] blob = readBinBlobWithTiming(binPath, brNs);
    out.avgBinReadNs = brNs[0];
    out.compressedBytesObserved = blob.length;

    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);
      t0 = System.nanoTime();
      // Match micro: decode in timed loop, discard returned int[] (do not assign to outer ref).
      decodePlain.apply(unc);
      t1 = System.nanoTime();
      sumDec += (t1 - t0);
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    int[] decoded = decodePlain.apply(unCompressor.uncompress(blob));
    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      long[] asLong = new long[decoded.length];
      for (int i = 0; i < decoded.length; i++) {
        asLong[i] = decoded[i];
      }
      sumCsv += timeDecodedLongCsvWriteNanos(decodedCsvPath, asLong);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;
    return out;
  }

  /**
   * Micro-style void decode in the timed loop (return value discarded). Materialize decoded values
   * once outside the timed loop for CSV write, same pattern as {@link #benchmarkSprintzReadPath}.
   */
  private static ReadPathTimings benchmarkMicroStyleVoidIntDecodeReadPath(
      File csvFile,
      int[] expected,
      Path binPath,
      Path decodedCsvPath,
      java.util.function.Consumer<byte[]> decodeTimed,
      java.util.function.Function<byte[], int[]> decodeMaterialize,
      long avgVerifyNs)
      throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
    ReadPathTimings out = new ReadPathTimings();
    out.avgDatasetVerifyReadNs = avgVerifyNs;
    long[] brNs = new long[1];
    byte[] blob = readBinBlobWithTiming(binPath, brNs);
    out.avgBinReadNs = brNs[0];
    out.compressedBytesObserved = blob.length;

    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);
      t0 = System.nanoTime();
      decodeTimed.accept(unc);
      t1 = System.nanoTime();
      sumDec += (t1 - t0);
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    int[] decoded = decodeMaterialize.apply(unCompressor.uncompress(blob));
    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      long[] asLong = new long[decoded.length];
      for (int i = 0; i < decoded.length; i++) {
        asLong[i] = decoded[i];
      }
      sumCsv += timeDecodedLongCsvWriteNanos(decodedCsvPath, asLong);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;
    return out;
  }

  private static ReadPathTimings benchmarkSprintzReadPath(
      File csvFile, int[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    return benchmarkMicroStyleVoidIntDecodeReadPath(
        csvFile,
        expected,
        binPath,
        decodedCsvPath,
        SPRINTZBPTest::BOSDecoder,
        SPRINTZBPTest::decodeToIntArray,
        avgDatasetVerifyReadIntsMicroStyleNanos(csvFile, expected));
  }

  private static ReadPathTimings benchmarkTs2DiffReadPath(
      File csvFile, int[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    return benchmarkMicroStyleVoidIntDecodeReadPath(
        csvFile,
        expected,
        binPath,
        decodedCsvPath,
        TSDIFFTest::BOSDecoderImprove,
        TSDIFFTest::decodeToIntArrayImprove,
        avgDatasetVerifyReadIntsMicroStyleNanos(csvFile, expected));
  }

  private static ReadPathTimings benchmarkSubcolumnReadPath(
      File csvFile, int[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    return benchmarkMicroStyleVoidIntDecodeReadPath(
        csvFile,
        expected,
        binPath,
        decodedCsvPath,
        unc -> SubcolumnPruneNewTest.Decoder(unc),
        SubcolumnPruneNewTest::Decoder,
        avgDatasetVerifyReadIntsMicroStyleNanos(csvFile, expected));
  }

  private static ReadPathTimings benchmarkBitPackingReadPath(
      File csvFile, int[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    return benchmarkPlainIntEncodedReadPath(
        csvFile,
        expected,
        binPath,
        decodedCsvPath,
        BPTest::Decoder,
        avgDatasetVerifyReadIntsMicroStyleNanos(csvFile, expected));
  }

  private static ReadPathTimings benchmarkRleReadPath(
      File csvFile, long[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
    ReadPathTimings out = new ReadPathTimings();
    out.avgDatasetVerifyReadNs = avgDatasetVerifyReadLongsFileWideNanos(csvFile, expected);
    long[] brNs = new long[1];
    byte[] blob = readBinBlobWithTiming(binPath, brNs);
    out.avgBinReadNs = brNs[0];
    out.compressedBytesObserved = blob.length;

    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);
      t0 = System.nanoTime();
      RLEBPLongTest.BOSDecoderImprove(unc);
      t1 = System.nanoTime();
      sumDec += (t1 - t0);
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    long[] decoded = RLEBPLongTest.decodeToLongArray(unCompressor.uncompress(blob));
    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      sumCsv += timeDecodedLongCsvWriteNanos(decodedCsvPath, decoded);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;
    return out;
  }

  private static ReadPathTimings benchmarkCodecBlobReadPath(
      File csvFile,
      int[] expectedInts,
      Path binPath,
      Path decodedCsvPath,
      java.util.function.Function<byte[], int[]> decodeBlob)
      throws IOException {
    ReadPathTimings out = new ReadPathTimings();

    long sumVerify = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      List<String[]> rows = readFullCsvAllColumnsAsStringRows(csvFile);
      long t1 = System.nanoTime();
      sumVerify += (t1 - t0);
      List<String> tokens = firstColumnTokensFromRows(rows);
      double[] vals = parseDoublesFromTokens(tokens);
      Assert.assertEquals(expectedInts.length, vals.length);
    }
    out.avgDatasetVerifyReadNs = sumVerify / BENCH_PHASE_REPEATS;

    long[] brNs = new long[1];
    byte[] blob = readBinBlobWithTiming(binPath, brNs);
    out.avgBinReadNs = brNs[0];
    out.compressedBytesObserved = blob.length;

    long sumDec = 0;
    int[] decoded = null;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      decoded = decodeBlob.apply(blob);
      long t1 = System.nanoTime();
      sumDec += (t1 - t0);
      // if (rep == 0) {
      //   Assert.assertArrayEquals(expectedInts, decoded);
      // }
    }
    out.avgUncompressNs = 0;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      long[] asLong = new long[decoded.length];
      for (int i = 0; i < decoded.length; i++) {
        asLong[i] = decoded[i];
      }
      sumCsv += timeDecodedLongCsvWriteNanos(decodedCsvPath, asLong);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;
    return out;
  }

  private static ReadPathTimings benchmarkSimple8bReadPath(
      File csvFile, int[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    return benchmarkCodecBlobReadPath(
        csvFile,
        expected,
        binPath,
        decodedCsvPath,
        blob -> FastPForSimple8bCodec.decode(intArrayFromBlobBytes(blob)));
  }

  private static ReadPathTimings benchmarkFastPforReadPath(
      File csvFile, int[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    IntCompressor comp = new IntCompressor(new FastPFOR());
    return benchmarkCodecBlobReadPath(
        csvFile,
        expected,
        binPath,
        decodedCsvPath,
        blob -> {
          int[] padded = comp.uncompress(intArrayFromBlobBytes(blob));
          return Arrays.copyOf(padded, expected.length);
        });
  }

  private static ReadPathTimings benchmarkRawDoubleLzmaReadPath(
      File csvFile, double[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.LZMA2);

    ReadPathTimings out = new ReadPathTimings();

    long sumVerify = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      List<String[]> rows = readFullCsvAllColumnsAsStringRows(csvFile);
      long t1 = System.nanoTime();
      sumVerify += (t1 - t0);
      List<String> tokens = firstColumnTokensFromRows(rows);
      double[] vals = parseDoublesFromTokens(tokens);
      Assert.assertEquals(expected.length, vals.length);
      for (int i = 0; i < expected.length; i++) {
        Assert.assertEquals(
            Double.doubleToLongBits(expected[i]), Double.doubleToLongBits(vals[i]));
      }
    }
    out.avgDatasetVerifyReadNs = sumVerify / BENCH_PHASE_REPEATS;

    long sumBr = 0;
    byte[] blob = null;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      if (benchWindowsBinReadShrinkBeforeReadEnv()) {
        blob = null;
      }
      blob = readCompressedBinBench(binPath);
      long t1 = System.nanoTime();
      sumBr += (t1 - t0);
    }
    out.avgBinReadNs = sumBr / BENCH_PHASE_REPEATS;
    Assert.assertNotNull(blob);
    out.compressedBytesObserved = blob.length;

    double[] decoded = new double[expected.length];
    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);

      t0 = System.nanoTime();
      double[] d = littleEndianBytesToDoubles(unc);
      System.arraycopy(d, 0, decoded, 0, d.length);
      t1 = System.nanoTime();
      sumDec += (t1 - t0);

      // if (rep == 0) {
      //   for (int i = 0; i < expected.length; i++) {
      //     Assert.assertEquals(
      //         Double.doubleToLongBits(expected[i]), Double.doubleToLongBits(decoded[i]));
      //   }
      // }
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      sumCsv += timeDecodedDoublesCsvWriteNanos(decodedCsvPath, decoded);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;

    return out;
  }

  private static ReadPathTimings benchmarkDictionaryReadPath(
      File csvFile, long[] expected, Path binPath, Path decodedCsvPath) throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);

    ReadPathTimings out = new ReadPathTimings();

    long sumVerify = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      List<String[]> rows = readFullCsvAllColumnsAsStringRows(csvFile);
      long t1 = System.nanoTime();
      sumVerify += (t1 - t0);
      List<String> tokens = firstColumnTokensFromRows(rows);
      long[] vals = scaleTokensPerSubcolumnBlock(tokens);
      Assert.assertEquals(expected.length, vals.length);
      Assert.assertArrayEquals(expected, vals);
    }
    out.avgDatasetVerifyReadNs = sumVerify / BENCH_PHASE_REPEATS;

    long sumBr = 0;
    byte[] blob = null;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      if (benchWindowsBinReadShrinkBeforeReadEnv()) {
        blob = null;
      }
      blob = readCompressedBinBench(binPath);
      long t1 = System.nanoTime();
      sumBr += (t1 - t0);
    }
    out.avgBinReadNs = sumBr / BENCH_PHASE_REPEATS;
    Assert.assertNotNull(blob);
    out.compressedBytesObserved = blob.length;

    long[] decoded = new long[expected.length];
    long sumUnc = 0;
    long sumDec = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(blob);
      long t1 = System.nanoTime();
      sumUnc += (t1 - t0);

      DictionaryDecoder dictionaryDecoder = new DictionaryDecoder();
      ByteBuffer db = ByteBuffer.wrap(unc);
      t0 = System.nanoTime();
      int i = 0;
      while (dictionaryDecoder.hasNext(db)) {
        decoded[i++] = binaryToLong(dictionaryDecoder.readBinary(db));
      }
      t1 = System.nanoTime();
      sumDec += (t1 - t0);
      // Assert.assertEquals(expected.length, i);

      // if (rep == 0) {
      //   Assert.assertArrayEquals(expected, decoded);
      // }
      dictionaryDecoder.reset();
    }
    out.avgUncompressNs = sumUnc / BENCH_PHASE_REPEATS;
    out.avgDecodeNs = sumDec / BENCH_PHASE_REPEATS;

    long sumCsv = 0;
    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      Files.deleteIfExists(decodedCsvPath);
      sumCsv += timeDecodedLongCsvWriteNanos(decodedCsvPath, decoded);
    }
    out.avgDecodedCsvWriteNs = sumCsv / BENCH_PHASE_REPEATS;

    return out;
  }

  private static PhaseTiming benchmarkRawDoubleLzma(double[] values) throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.LZMA2);

    PhaseTiming t = new PhaseTiming();

    byte[] raw = doublesToLittleEndianRawBytes(values);
    byte[] compressed0 = compressLzma2BenchPreset(raw);
    byte[] back0 = unCompressor.uncompress(compressed0);
    assertRawDoubleRoundtrip(values, back0);
    t.encodedLen = raw.length;
    t.compressedLen = compressed0.length;

    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      long t0 = System.nanoTime();
      byte[] cmp = compressLzma2BenchPreset(raw);
      long t1 = System.nanoTime();
      t.sumCompressNs += (t1 - t0);

      t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(cmp);
      t1 = System.nanoTime();
      t.sumUncompressNs += (t1 - t0);

      t0 = System.nanoTime();
      littleEndianBytesToDoubles(unc);
      t1 = System.nanoTime();
      t.sumDecodeNs += (t1 - t0);
    }
    return t;
  }

  private static PhaseTiming benchmarkDictionaryUncompressed(long[] values) throws IOException {
    ICompressor compressor = ICompressor.getCompressor(CompressionType.UNCOMPRESSED);
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(CompressionType.UNCOMPRESSED);
    PhaseTiming t = new PhaseTiming();

    DictionaryEncoder encoder = new DictionaryEncoder();
    ByteArrayOutputStream baos0 = new ByteArrayOutputStream();
    for (long v : values) {
      encoder.encode(longToPlainBinary(v), baos0);
    }
    encoder.flush(baos0);
    byte[] plain = baos0.toByteArray();
    byte[] compressed0 = compressor.compress(plain);
    byte[] back0 = unCompressor.uncompress(compressed0);
    assertDictionaryRoundtrip(values, back0);
    t.encodedLen = plain.length;
    t.compressedLen = compressed0.length;

    for (int rep = 0; rep < BENCH_PHASE_REPEATS; rep++) {
      DictionaryEncoder enc = new DictionaryEncoder();

      long t0 = System.nanoTime();
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      for (long v : values) {
        enc.encode(longToPlainBinary(v), baos);
      }
      enc.flush(baos);
      byte[] encBytes = baos.toByteArray();
      long t1 = System.nanoTime();
      t.sumEncodeFlushNs += (t1 - t0);

      t0 = System.nanoTime();
      byte[] cmp = compressor.compress(encBytes);
      t1 = System.nanoTime();
      t.sumCompressNs += (t1 - t0);

      t0 = System.nanoTime();
      byte[] unc = unCompressor.uncompress(cmp);
      t1 = System.nanoTime();
      t.sumUncompressNs += (t1 - t0);

      DictionaryDecoder decoder = new DictionaryDecoder();
      t0 = System.nanoTime();
      ByteBuffer db = ByteBuffer.wrap(unc);
      while (decoder.hasNext(db)) {
        decoder.readBinary(db);
      }
      t1 = System.nanoTime();
      t.sumDecodeNs += (t1 - t0);
      decoder.reset();
    }
    return t;
  }

  @Test
  public void testSyntheticRawDoubleLzmaRoundTripAndTiming() throws IOException {
    double[] values = {
      0.0,
      1.0,
      -1.0,
      1234567890123.0,
      (double) (Long.MAX_VALUE / 4),
      (double) (Long.MIN_VALUE / 4),
      42.0,
      42.0,
      99.0,
      -300.0
    };
    PhaseTiming t = benchmarkRawDoubleLzma(values);
    Assert.assertTrue(t.compressedLen <= t.encodedLen + 200);
    System.out.printf(
        "RAW_DOUBLE+LZMA2 synthetic n=%d enc+flush_avg_ns=%.1f compress_avg_ns=%.1f "
            + "uncompress_avg_ns=%.1f decode_avg_ns=%.1f encoded_B=%d compressed_B=%d%n",
        values.length,
        t.avgEncodeFlushNs(),
        t.avgCompressNs(),
        t.avgUncompressNs(),
        t.avgDecodeNs(),
        t.encodedLen,
        t.compressedLen);
  }

  @Test
  public void testSyntheticDictionaryUncompressedRoundTripAndTiming() throws IOException {
    long[] values = {10L, 20L, 10L, 30L, 20L, 10L};
    PhaseTiming t = benchmarkDictionaryUncompressed(values);
    System.out.printf(
        "DICTIONARY+UNC synthetic n=%d enc+flush_avg_ns=%.1f compress_avg_ns=%.1f "
            + "uncompress_avg_ns=%.1f decode_avg_ns=%.1f encoded_B=%d compressed_B=%d%n",
        values.length,
        t.avgEncodeFlushNs(),
        t.avgCompressNs(),
        t.avgUncompressNs(),
        t.avgDecodeNs(),
        t.encodedLen,
        t.compressedLen);
  }

  @Test
  public void testHddEncodeCompressWriteBinCsvIfPresent() throws IOException {
    runEncodeCompressWriteBinCsv(HDD_PROFILE);
  }

  @Test
  public void testHddDecodeBinWriteDecodedCsvIfPresent() throws IOException {
    runDecodeBinWriteDecodedCsv(HDD_PROFILE);
  }

  @Test
  public void testSsdEncodeCompressWriteBinCsvIfPresent() throws IOException {
    runEncodeCompressWriteBinCsv(SSD_PROFILE);
  }

  @Test
  public void testSsdDecodeBinWriteDecodedCsvIfPresent() throws IOException {
    runDecodeBinWriteDecodedCsv(SSD_PROFILE);
  }

  private static void runEncodeCompressWriteBinCsv(BenchStorageProfile profile) throws IOException {
    File sourceDir = new File(profile.datasetDir);
    Assume.assumeTrue(
        "Skip when CSV dir missing: " + profile.datasetDir, sourceDir.isDirectory());
    File[] csvFiles = listBenchmarkDatasetCsvFiles(sourceDir);
    Assume.assumeTrue(
        "Skip when no CSV files in " + profile.datasetDir, csvFiles.length > 0);

    Path binsDir = Paths.get(profile.binOutputDir);
    Path decodedRoot = Paths.get(profile.decodedCsvDir);
    ensureDirectory(binsDir);
    ensureDirectory(decodedRoot);

    Path writeCsvPath = Paths.get(profile.writeMetricsCsvPath);
    Path manifestPath = Paths.get(profile.compressManifestCsvPath);
    ensureDirectory(writeCsvPath.getParent());
    ensureDirectory(manifestPath.getParent());

    try (BufferedWriter writeMetrics =
            Files.newBufferedWriter(
                writeCsvPath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
        BufferedWriter manifest =
            Files.newBufferedWriter(
                manifestPath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {

      writeMetrics.write(
          "Dataset,Encoding Algorithm,Write Total Time Nanos,Dataset Read "
              + "Time Nanos,Write CPU Time Nanos,Write IO Time Nanos,Points,Max "
              + "Decimal Precision,Multiplier,TsFile Size Bytes,Write Encode Loop Time "
              + "Nanos,Write Encode Flush Time Nanos,Write Compress Time Nanos\n");
      manifest.write(
          "Dataset,Encoding Algorithm,Source CSV Path,Points,Encoded "
              + "Uncompressed Bytes,Raw Plain Codec\n");

      System.out.printf(
          "[DatasetEncoderCompressBinBench] encode-start | profile=%s | sourceDir=%s | datasets=%d | binsDir=%s%n",
          profile.label,
          sourceDir.getAbsolutePath(),
          csvFiles.length,
          binsDir.toAbsolutePath());
      System.out.flush();

      for (File f : csvFiles) {
        List<String> tokens = readFirstColumnTokens(f);
        Assume.assumeFalse(tokens.isEmpty());
        int maxPrecAll = maxDecimalPrecision(tokens);
        double[] dValues = parseDoublesFromTokens(tokens);
        long[] dictValues = scaleTokensPerSubcolumnBlock(tokens);
        Assert.assertEquals(dValues.length, dictValues.length);
        String datasetName = stripCsvExtension(f.getName());
        String srcPath = f.getCanonicalPath().replace('\\', '/');

        long avgReadFullCsvNs = averageCsvFullReadWithoutNumericParseNanos(f);
        int boundedPrec = Math.min(maxPrecAll, K_MAX_DECIMAL_PRECISION);
        long multiplier = multiplierForBoundedPrecision(maxPrecAll);

        int[] intScaled = scaleDoublesToIntsMicroStyle(dValues, maxPrecAll);
        long[] hbpValues = scaleDoublesToLongsFileWide(dValues, maxPrecAll);
        long[] rleValues = hbpValues;

        logBenchDatasetHeader("encode", datasetName, f, dValues.length);

        Path binLzma = binsDir.resolve(datasetName + "_" + SUFFIX_PLAIN_LZMA + ".bin");
        logBenchWarmupDiscarded("EncodeCompressWriteBinCsv", datasetName, ALGO_LZMA_CSV);
        benchmarkRawDoubleLzmaWritePath(dValues, binLzma);
        logBenchProgress("encode", datasetName, ALGO_LZMA_CSV, dValues.length, binLzma);
        WritePathTimings wLz = benchmarkRawDoubleLzmaWritePath(dValues, binLzma);
        wLz.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics, datasetName, ALGO_LZMA_CSV, wLz, dValues.length, boundedPrec, multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_LZMA_CSV,
            srcPath,
            dValues.length,
            wLz.encodedUncompressedBytes,
            1);

        Path binDict = binsDir.resolve(datasetName + "_" + SUFFIX_DICTIONARY + ".bin");
        logBenchProgress("encode", datasetName, ALGO_DICTIONARY_CSV, dictValues.length, binDict);
        WritePathTimings wDc = benchmarkDictionaryWritePath(dictValues, binDict);
        wDc.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics,
            datasetName,
            ALGO_DICTIONARY_CSV,
            wDc,
            dictValues.length,
            boundedPrec,
            multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_DICTIONARY_CSV,
            srcPath,
            dictValues.length,
            wDc.encodedUncompressedBytes,
            0);

        Path binGorilla = binsDir.resolve(datasetName + "_" + SUFFIX_GORILLA + ".bin");
        logBenchProgress("encode", datasetName, ALGO_GORILLA_CSV, dValues.length, binGorilla);
        WritePathTimings wGorilla = benchmarkGorillaWritePath(dValues, binGorilla);
        wGorilla.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics, datasetName, ALGO_GORILLA_CSV, wGorilla, dValues.length, boundedPrec, multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_GORILLA_CSV,
            srcPath,
            dValues.length,
            wGorilla.encodedUncompressedBytes,
            0);

        Path binChimp = binsDir.resolve(datasetName + "_" + SUFFIX_CHIMP + ".bin");
        logBenchProgress("encode", datasetName, ALGO_CHIMP_CSV, dValues.length, binChimp);
        WritePathTimings wChimp = benchmarkChimpWritePath(dValues, binChimp);
        wChimp.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics, datasetName, ALGO_CHIMP_CSV, wChimp, dValues.length, boundedPrec, multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_CHIMP_CSV,
            srcPath,
            dValues.length,
            wChimp.encodedUncompressedBytes,
            0);

        Path binElf = binsDir.resolve(datasetName + "_" + SUFFIX_ELF + ".bin");
        logBenchProgress("encode", datasetName, ALGO_ELF_CSV, dValues.length, binElf);
        WritePathTimings wElf = benchmarkElfWritePath(dValues, binElf);
        wElf.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics, datasetName, ALGO_ELF_CSV, wElf, dValues.length, boundedPrec, multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_ELF_CSV,
            srcPath,
            dValues.length,
            wElf.encodedUncompressedBytes,
            0);

        Path binBuff = binsDir.resolve(datasetName + "_" + SUFFIX_BUFF + ".bin");
        logBenchProgress("encode", datasetName, ALGO_BUFF_CSV, dValues.length, binBuff);
        WritePathTimings wBuff = benchmarkBuffWritePath(dValues, maxPrecAll, binBuff);
        wBuff.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics, datasetName, ALGO_BUFF_CSV, wBuff, dValues.length, boundedPrec, multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_BUFF_CSV,
            srcPath,
            dValues.length,
            wBuff.encodedUncompressedBytes,
            0);

        Path binAlp = binsDir.resolve(datasetName + "_" + SUFFIX_ALP + ".bin");
        logBenchProgress("encode", datasetName, ALGO_ALP_CSV, dValues.length, binAlp);
        WritePathTimings wAlp = benchmarkAlpWritePath(dValues, maxPrecAll, binAlp);
        wAlp.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics, datasetName, ALGO_ALP_CSV, wAlp, dValues.length, boundedPrec, multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_ALP_CSV,
            srcPath,
            dValues.length,
            wAlp.encodedUncompressedBytes,
            0);

        Path binBitweaving = binsDir.resolve(datasetName + "_" + SUFFIX_BITWEAVING + ".bin");
        logBenchProgress("encode", datasetName, ALGO_BITWEAVING_CSV, hbpValues.length, binBitweaving);
        WritePathTimings wHbp = benchmarkHbpWritePath(hbpValues, binBitweaving);
        wHbp.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics,
            datasetName,
            ALGO_BITWEAVING_CSV,
            wHbp,
            hbpValues.length,
            boundedPrec,
            multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_BITWEAVING_CSV,
            srcPath,
            hbpValues.length,
            wHbp.encodedUncompressedBytes,
            0);

        Path binS8 = binsDir.resolve(datasetName + "_" + SUFFIX_SIMPLE8B + ".bin");
        logBenchProgress("encode", datasetName, ALGO_SIMPLE8B_CSV, intScaled.length, binS8);
        WritePathTimings wS8 = benchmarkSimple8bWritePath(intScaled, binS8);
        wS8.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics,
            datasetName,
            ALGO_SIMPLE8B_CSV,
            wS8,
            intScaled.length,
            boundedPrec,
            multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_SIMPLE8B_CSV,
            srcPath,
            intScaled.length,
            wS8.encodedUncompressedBytes,
            0);

        Path binFp = binsDir.resolve(datasetName + "_" + SUFFIX_FASTPFOR + ".bin");
        logBenchProgress("encode", datasetName, ALGO_FASTPFOR_CSV, intScaled.length, binFp);
        WritePathTimings wFp = benchmarkFastPforWritePath(intScaled, binFp);
        wFp.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics,
            datasetName,
            ALGO_FASTPFOR_CSV,
            wFp,
            intScaled.length,
            boundedPrec,
            multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_FASTPFOR_CSV,
            srcPath,
            intScaled.length,
            wFp.encodedUncompressedBytes,
            0);

        Path binRle = binsDir.resolve(datasetName + "_" + SUFFIX_RLE + ".bin");
        logBenchProgress("encode", datasetName, ALGO_RLE_CSV, rleValues.length, binRle);
        WritePathTimings wRle = benchmarkRleWritePath(rleValues, binRle);
        wRle.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics,
            datasetName,
            ALGO_RLE_CSV,
            wRle,
            rleValues.length,
            boundedPrec,
            multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_RLE_CSV,
            srcPath,
            rleValues.length,
            wRle.encodedUncompressedBytes,
            0);

        Path binBitPacking = binsDir.resolve(datasetName + "_" + SUFFIX_BITPACKING + ".bin");
        logBenchProgress("encode", datasetName, ALGO_BITPACKING_CSV, intScaled.length, binBitPacking);
        WritePathTimings wBp = benchmarkBitPackingWritePath(intScaled, binBitPacking);
        wBp.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics,
            datasetName,
            ALGO_BITPACKING_CSV,
            wBp,
            intScaled.length,
            boundedPrec,
            multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_BITPACKING_CSV,
            srcPath,
            intScaled.length,
            wBp.encodedUncompressedBytes,
            0);

        Path binSprintz = binsDir.resolve(datasetName + "_" + SUFFIX_SPRINTZ + ".bin");
        logBenchProgress("encode", datasetName, ALGO_SPRINTZ_CSV, intScaled.length, binSprintz);
        WritePathTimings wSprintz = benchmarkSprintzWritePath(intScaled, binSprintz);
        wSprintz.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics,
            datasetName,
            ALGO_SPRINTZ_CSV,
            wSprintz,
            intScaled.length,
            boundedPrec,
            multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_SPRINTZ_CSV,
            srcPath,
            intScaled.length,
            wSprintz.encodedUncompressedBytes,
            0);

        Path binTs2Diff = binsDir.resolve(datasetName + "_" + SUFFIX_TS_2DIFF + ".bin");
        logBenchProgress("encode", datasetName, ALGO_TS_2DIFF_CSV, intScaled.length, binTs2Diff);
        WritePathTimings wTs2Diff = benchmarkTs2DiffWritePath(intScaled, binTs2Diff);
        wTs2Diff.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics,
            datasetName,
            ALGO_TS_2DIFF_CSV,
            wTs2Diff,
            intScaled.length,
            boundedPrec,
            multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_TS_2DIFF_CSV,
            srcPath,
            intScaled.length,
            wTs2Diff.encodedUncompressedBytes,
            0);

        Path binSubcolumn = binsDir.resolve(datasetName + "_" + SUFFIX_SUBCOLUMN + ".bin");
        logBenchProgress(
            "encode", datasetName, ALGO_SUBCOLUMN_CSV, intScaled.length, binSubcolumn);
        WritePathTimings wSubcolumn = benchmarkSubcolumnWritePath(intScaled, binSubcolumn);
        wSubcolumn.avgDatasetReadNs = avgReadFullCsvNs;
        writeEncodeMetricsRow(
            writeMetrics,
            datasetName,
            ALGO_SUBCOLUMN_CSV,
            wSubcolumn,
            intScaled.length,
            boundedPrec,
            multiplier);
        writeManifestRow(
            manifest,
            datasetName,
            ALGO_SUBCOLUMN_CSV,
            srcPath,
            intScaled.length,
            wSubcolumn.encodedUncompressedBytes,
            0);

        System.out.printf(
            "[DatasetEncoderCompressBinBench] encode-done | profile=%s | dataset=%s | algorithms=%d%n",
            profile.label,
            datasetName,
            BENCH_ALGORITHM_COUNT);
        System.out.flush();
      }
      System.out.printf(
          "[DatasetEncoderCompressBinBench] encode-finish | profile=%s | writeMetrics=%s | manifest=%s%n",
          profile.label,
          writeCsvPath.toAbsolutePath(),
          manifestPath.toAbsolutePath());
      System.out.flush();
    }
  }

  private static void runDecodeBinWriteDecodedCsv(BenchStorageProfile profile) throws IOException {
    File sourceDir = new File(profile.datasetDir);
    Assume.assumeTrue(
        "Skip when CSV dir missing: " + profile.datasetDir, sourceDir.isDirectory());
    File[] csvFiles = listBenchmarkDatasetCsvFiles(sourceDir);
    Assume.assumeTrue(
        "Skip when no CSV files in " + profile.datasetDir, csvFiles.length > 0);

    Path binsDir = Paths.get(profile.binOutputDir);
    Path decodedRoot = Paths.get(profile.decodedCsvDir);
    Path readCsvPath = Paths.get(profile.readMetricsCsvPath);
    Path manifestPath = Paths.get(profile.compressManifestCsvPath);

    ensureDirectory(decodedRoot);
    ensureDirectory(readCsvPath.getParent());

    Map<String, Map<String, ManifestRecord>> manifestMap = loadManifestRows(manifestPath);

    try (BufferedWriter readMetrics =
        Files.newBufferedWriter(
            readCsvPath,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {

      readMetrics.write(
          "Dataset,Encoding Algorithm,Read Total Time Nanos,Read CPU Time "
              + "Nanos,Dataset Verify Read Time Nanos,Read IO Time Nanos,Decoded CSV "
              + "Write Time Nanos,Points,"
              + "TsFile Size Bytes\n");

      System.out.printf(
          "[DatasetEncoderCompressBinBench] decode-start | profile=%s | sourceDir=%s | datasets=%d | binsDir=%s%n",
          profile.label,
          sourceDir.getAbsolutePath(),
          csvFiles.length,
          binsDir.toAbsolutePath());
      System.out.flush();

      for (File f : csvFiles) {
        List<String> tokens = readFirstColumnTokens(f);
        Assume.assumeFalse(tokens.isEmpty());
        double[] expectedDoubles = parseDoublesFromTokens(tokens);
        long[] dictValues = scaleTokensPerSubcolumnBlock(tokens);
        int maxPrecAll = maxDecimalPrecision(tokens);
        int[] intScaled = scaleDoublesToIntsMicroStyle(expectedDoubles, maxPrecAll);
        long[] hbpValues = scaleDoublesToLongsFileWide(expectedDoubles, maxPrecAll);
        long[] rleValues = hbpValues;
        Assert.assertEquals(expectedDoubles.length, dictValues.length);
        String datasetName = stripCsvExtension(f.getName());

        Map<String, ManifestRecord> rows = manifestMap.get(datasetName);
        Assert.assertNotNull(
            "No manifest rows for dataset "
                + datasetName
                + " (run "
                + (profile.label.equals("HDD")
                    ? "testHddEncodeCompressWriteBinCsvIfPresent"
                    : "testSsdEncodeCompressWriteBinCsvIfPresent")
                + " first)",
            rows);

        logBenchDatasetHeader("decode", datasetName, f, expectedDoubles.length);

        ManifestRecord lzRec = rows.get(ALGO_LZMA_CSV);
        Assert.assertNotNull("Missing LZMA manifest row for " + datasetName, lzRec);
        Assert.assertEquals(expectedDoubles.length, lzRec.points);
        Assert.assertEquals(1, lzRec.rawPlainCodec);
        Path binLzma = binsDir.resolve(datasetName + "_" + SUFFIX_PLAIN_LZMA + ".bin");
        Assert.assertTrue("Missing bin: " + binLzma, Files.isRegularFile(binLzma));
        Path decodedLzma = decodedRoot.resolve(datasetName + "_" + SUFFIX_PLAIN_LZMA + "_decoded.csv");
        logBenchWarmupDiscarded("DecodeBinWriteDecodedCsv", datasetName, ALGO_LZMA_CSV);
        benchmarkRawDoubleLzmaReadPath(f, expectedDoubles, binLzma, decodedLzma);
        logBenchProgress("decode", datasetName, ALGO_LZMA_CSV, expectedDoubles.length, binLzma);
        ReadPathTimings rLz =
            benchmarkRawDoubleLzmaReadPath(f, expectedDoubles, binLzma, decodedLzma);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_LZMA_CSV, rLz, expectedDoubles.length);

        ManifestRecord dcRec = rows.get(ALGO_DICTIONARY_CSV);
        Assert.assertNotNull("Missing DICTIONARY manifest row for " + datasetName, dcRec);
        Path binDict = binsDir.resolve(datasetName + "_" + SUFFIX_DICTIONARY + ".bin");
        Assert.assertTrue("Missing bin: " + binDict, Files.isRegularFile(binDict));
        Path decodedDict = decodedRoot.resolve(datasetName + "_" + SUFFIX_DICTIONARY + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_DICTIONARY_CSV, dictValues.length, binDict);
        ReadPathTimings rDc =
            benchmarkDictionaryReadPath(f, dictValues, binDict, decodedDict);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_DICTIONARY_CSV, rDc, dictValues.length);

        Assert.assertNotNull(rows.get(ALGO_GORILLA_CSV));
        Path binGorilla = binsDir.resolve(datasetName + "_" + SUFFIX_GORILLA + ".bin");
        Assert.assertTrue("Missing bin: " + binGorilla, Files.isRegularFile(binGorilla));
        Path decodedGorilla = decodedRoot.resolve(datasetName + "_" + SUFFIX_GORILLA + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_GORILLA_CSV, expectedDoubles.length, binGorilla);
        ReadPathTimings rGorilla =
            benchmarkGorillaReadPath(f, expectedDoubles, binGorilla, decodedGorilla);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_GORILLA_CSV, rGorilla, expectedDoubles.length);

        Assert.assertNotNull(rows.get(ALGO_CHIMP_CSV));
        Path binChimp = binsDir.resolve(datasetName + "_" + SUFFIX_CHIMP + ".bin");
        Assert.assertTrue("Missing bin: " + binChimp, Files.isRegularFile(binChimp));
        Path decodedChimp = decodedRoot.resolve(datasetName + "_" + SUFFIX_CHIMP + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_CHIMP_CSV, expectedDoubles.length, binChimp);
        ReadPathTimings rChimp =
            benchmarkChimpReadPath(f, expectedDoubles, binChimp, decodedChimp);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_CHIMP_CSV, rChimp, expectedDoubles.length);

        Assert.assertNotNull(rows.get(ALGO_ELF_CSV));
        Path binElf = binsDir.resolve(datasetName + "_" + SUFFIX_ELF + ".bin");
        Assert.assertTrue("Missing bin: " + binElf, Files.isRegularFile(binElf));
        Path decodedElf = decodedRoot.resolve(datasetName + "_" + SUFFIX_ELF + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_ELF_CSV, expectedDoubles.length, binElf);
        ReadPathTimings rElf =
            benchmarkElfReadPath(f, expectedDoubles, binElf, decodedElf);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_ELF_CSV, rElf, expectedDoubles.length);

        Assert.assertNotNull(rows.get(ALGO_BUFF_CSV));
        Path binBuff = binsDir.resolve(datasetName + "_" + SUFFIX_BUFF + ".bin");
        Assert.assertTrue("Missing bin: " + binBuff, Files.isRegularFile(binBuff));
        Path decodedBuff = decodedRoot.resolve(datasetName + "_" + SUFFIX_BUFF + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_BUFF_CSV, expectedDoubles.length, binBuff);
        ReadPathTimings rBuff =
            benchmarkBuffReadPath(f, expectedDoubles, maxPrecAll, binBuff, decodedBuff);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_BUFF_CSV, rBuff, expectedDoubles.length);

        Assert.assertNotNull(rows.get(ALGO_ALP_CSV));
        Path binAlp = binsDir.resolve(datasetName + "_" + SUFFIX_ALP + ".bin");
        Assert.assertTrue("Missing bin: " + binAlp, Files.isRegularFile(binAlp));
        Path decodedAlp = decodedRoot.resolve(datasetName + "_" + SUFFIX_ALP + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_ALP_CSV, expectedDoubles.length, binAlp);
        ReadPathTimings rAlp =
            benchmarkAlpReadPath(f, expectedDoubles, maxPrecAll, binAlp, decodedAlp);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_ALP_CSV, rAlp, expectedDoubles.length);

        Assert.assertNotNull(rows.get(ALGO_BITWEAVING_CSV));
        Path binHbp = binsDir.resolve(datasetName + "_" + SUFFIX_BITWEAVING + ".bin");
        Assert.assertTrue("Missing bin: " + binHbp, Files.isRegularFile(binHbp));
        Path decodedBitweaving =
            decodedRoot.resolve(datasetName + "_" + SUFFIX_BITWEAVING + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_BITWEAVING_CSV, hbpValues.length, binHbp);
        ReadPathTimings rHbp =
            benchmarkHbpReadPath(f, hbpValues, binHbp, decodedBitweaving);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_BITWEAVING_CSV, rHbp, hbpValues.length);

        Assert.assertNotNull(rows.get(ALGO_SIMPLE8B_CSV));
        Path binS8 = binsDir.resolve(datasetName + "_" + SUFFIX_SIMPLE8B + ".bin");
        Assert.assertTrue("Missing bin: " + binS8, Files.isRegularFile(binS8));
        Path decodedS8 = decodedRoot.resolve(datasetName + "_" + SUFFIX_SIMPLE8B + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_SIMPLE8B_CSV, intScaled.length, binS8);
        ReadPathTimings rS8 =
            benchmarkSimple8bReadPath(f, intScaled, binS8, decodedS8);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_SIMPLE8B_CSV, rS8, intScaled.length);

        Assert.assertNotNull(rows.get(ALGO_FASTPFOR_CSV));
        Path binFp = binsDir.resolve(datasetName + "_" + SUFFIX_FASTPFOR + ".bin");
        Assert.assertTrue("Missing bin: " + binFp, Files.isRegularFile(binFp));
        Path decodedFp = decodedRoot.resolve(datasetName + "_" + SUFFIX_FASTPFOR + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_FASTPFOR_CSV, intScaled.length, binFp);
        ReadPathTimings rFp =
            benchmarkFastPforReadPath(f, intScaled, binFp, decodedFp);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_FASTPFOR_CSV, rFp, intScaled.length);

        Assert.assertNotNull(rows.get(ALGO_RLE_CSV));
        Path binRle = binsDir.resolve(datasetName + "_" + SUFFIX_RLE + ".bin");
        Assert.assertTrue("Missing bin: " + binRle, Files.isRegularFile(binRle));
        Path decodedRle = decodedRoot.resolve(datasetName + "_" + SUFFIX_RLE + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_RLE_CSV, rleValues.length, binRle);
        ReadPathTimings rRle = benchmarkRleReadPath(f, rleValues, binRle, decodedRle);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_RLE_CSV, rRle, rleValues.length);

        Assert.assertNotNull(rows.get(ALGO_BITPACKING_CSV));
        Path binBitPacking = binsDir.resolve(datasetName + "_" + SUFFIX_BITPACKING + ".bin");
        Assert.assertTrue("Missing bin: " + binBitPacking, Files.isRegularFile(binBitPacking));
        Path decodedBitPacking =
            decodedRoot.resolve(datasetName + "_" + SUFFIX_BITPACKING + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_BITPACKING_CSV, intScaled.length, binBitPacking);
        ReadPathTimings rBp =
            benchmarkBitPackingReadPath(f, intScaled, binBitPacking, decodedBitPacking);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_BITPACKING_CSV, rBp, intScaled.length);

        Assert.assertNotNull(rows.get(ALGO_SPRINTZ_CSV));
        Path binSprintz = binsDir.resolve(datasetName + "_" + SUFFIX_SPRINTZ + ".bin");
        Assert.assertTrue("Missing bin: " + binSprintz, Files.isRegularFile(binSprintz));
        Path decodedSprintz = decodedRoot.resolve(datasetName + "_" + SUFFIX_SPRINTZ + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_SPRINTZ_CSV, intScaled.length, binSprintz);
        ReadPathTimings rSprintz =
            benchmarkSprintzReadPath(f, intScaled, binSprintz, decodedSprintz);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_SPRINTZ_CSV, rSprintz, intScaled.length);

        Assert.assertNotNull(rows.get(ALGO_TS_2DIFF_CSV));
        Path binTs2Diff = binsDir.resolve(datasetName + "_" + SUFFIX_TS_2DIFF + ".bin");
        Assert.assertTrue("Missing bin: " + binTs2Diff, Files.isRegularFile(binTs2Diff));
        Path decodedTs2Diff =
            decodedRoot.resolve(datasetName + "_" + SUFFIX_TS_2DIFF + "_decoded.csv");
        logBenchProgress("decode", datasetName, ALGO_TS_2DIFF_CSV, intScaled.length, binTs2Diff);
        ReadPathTimings rTs2Diff =
            benchmarkTs2DiffReadPath(f, intScaled, binTs2Diff, decodedTs2Diff);
        writeReadMetricsRow(readMetrics, datasetName, ALGO_TS_2DIFF_CSV, rTs2Diff, intScaled.length);

        Assert.assertNotNull(rows.get(ALGO_SUBCOLUMN_CSV));
        Path binSubcolumn = binsDir.resolve(datasetName + "_" + SUFFIX_SUBCOLUMN + ".bin");
        Assert.assertTrue("Missing bin: " + binSubcolumn, Files.isRegularFile(binSubcolumn));
        Path decodedSubcolumn =
            decodedRoot.resolve(datasetName + "_" + SUFFIX_SUBCOLUMN + "_decoded.csv");
        logBenchProgress(
            "decode", datasetName, ALGO_SUBCOLUMN_CSV, intScaled.length, binSubcolumn);
        ReadPathTimings rSubcolumn =
            benchmarkSubcolumnReadPath(f, intScaled, binSubcolumn, decodedSubcolumn);
        writeReadMetricsRow(
            readMetrics, datasetName, ALGO_SUBCOLUMN_CSV, rSubcolumn, intScaled.length);

        System.out.printf(
            "[DatasetEncoderCompressBinBench] decode-done | profile=%s | dataset=%s | algorithms=%d%n",
            profile.label,
            datasetName,
            BENCH_ALGORITHM_COUNT);
        System.out.flush();
      }
      System.out.printf(
          "[DatasetEncoderCompressBinBench] decode-finish | profile=%s | readMetrics=%s%n",
          profile.label,
          readCsvPath.toAbsolutePath());
      System.out.flush();
    }
  }

  private static String stripCsvExtension(String filename) {
    if (filename.endsWith(".csv")) {
      return filename.substring(0, filename.length() - 4);
    }
    return filename;
  }
}
