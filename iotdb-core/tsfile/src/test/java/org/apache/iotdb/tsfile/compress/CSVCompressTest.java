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
package org.apache.iotdb.tsfile.compress;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

public class CSVCompressTest {

  private static final String PARENT_DIR = "D://github/xjz17/subcolumn/";
  private static final String INPUT_PARENT_DIR = PARENT_DIR + "dataset/";
  private static final String OUTPUT_PARENT_DIR = PARENT_DIR + "result/";
  private static final String[] HEADERS = {
    "Dataset",
    "Encoding Algorithm",
    "Encoding Time",
    "Decoding Time",
    "Points",
    "Original Size",
    "Compressed Size",
    "Compression Ratio"
  };

  private static class CompressionBenchmark {
    private final String algorithm;
    private final String outputFileName;
    private final Supplier<ICompressor> compressorSupplier;
    private final Supplier<IUnCompressor> unCompressorSupplier;
    private CsvWriter writer;

    private CompressionBenchmark(
        String algorithm,
        String outputFileName,
        Supplier<ICompressor> compressorSupplier,
        Supplier<IUnCompressor> unCompressorSupplier) {
      this.algorithm = algorithm;
      this.outputFileName = outputFileName;
      this.compressorSupplier = compressorSupplier;
      this.unCompressorSupplier = unCompressorSupplier;
    }
  }

  private static class BenchmarkResult {
    private final long encodeTime;
    private final long decodeTime;
    private final int compressedSize;
    private final double compressionRatio;

    private BenchmarkResult(
        long encodeTime, long decodeTime, int compressedSize, double compressionRatio) {
      this.encodeTime = encodeTime;
      this.decodeTime = decodeTime;
      this.compressedSize = compressedSize;
      this.compressionRatio = compressionRatio;
    }
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

    int REPEAT_TIME = 100;
    // REPEAT_TIME = 500;
    // REPEAT_TIME = 10;

    Files.createDirectories(Paths.get(OUTPUT_PARENT_DIR));

    File directory = new File(INPUT_PARENT_DIR);
    File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
    if (csvFiles == null || csvFiles.length == 0) {
      return;
    }
    Arrays.sort(csvFiles, Comparator.comparing(File::getName));

    List<CompressionBenchmark> benchmarks = new ArrayList<>();
    benchmarks.add(
        new CompressionBenchmark(
            "GZIP",
            "compress_gzip.csv",
            ICompressor.GZIPCompressor::new,
            IUnCompressor.GZIPUnCompressor::new));
    benchmarks.add(
        new CompressionBenchmark(
            "LZ4",
            "compress_lz4.csv",
            ICompressor.IOTDBLZ4Compressor::new,
            IUnCompressor.LZ4UnCompressor::new));
    benchmarks.add(
        new CompressionBenchmark(
            "SNAPPY",
            "compress_snappy.csv",
            ICompressor.SnappyCompressor::new,
            IUnCompressor.SnappyUnCompressor::new));
    benchmarks.add(
        new CompressionBenchmark(
            "LZMA2",
            "compress_lzma2.csv",
            ICompressor.LZMA2Compressor::new,
            IUnCompressor.LZMA2UnCompressor::new));
    benchmarks.add(
        new CompressionBenchmark(
            "ZSTD",
            "compress_zstd.csv",
            ICompressor.ZstdCompressor::new,
            IUnCompressor.ZstdUnCompressor::new));

    try {
      for (CompressionBenchmark benchmark : benchmarks) {
        benchmark.writer =
            new CsvWriter(
                OUTPUT_PARENT_DIR + benchmark.outputFileName, ',', StandardCharsets.UTF_8);
        benchmark.writer.setRecordDelimiter('\n');
        benchmark.writer.writeRecord(HEADERS);
      }

      for (File file : csvFiles) {
        String datasetName = extractFileName(file.toString());
        System.out.println(datasetName);

        int[] data;
        try (InputStream inputStream = Files.newInputStream(file.toPath())) {
          CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
          ArrayList<Float> floatData = new ArrayList<>();

          int maxDecimal = 0;
          while (loader.readRecord()) {
            String value = loader.getValues()[0];
            if (value.isEmpty()) {
              continue;
            }
            int currentDecimal = getDecimalPrecision(value);
            if (currentDecimal > maxDecimal) {
              maxDecimal = currentDecimal;
            }
            floatData.add(Float.valueOf(value));
          }
          loader.close();

          if (maxDecimal > 8) {
            maxDecimal = 8;
          }

          data = new int[floatData.size()];
          int maxMul = (int) Math.pow(10, maxDecimal);
          for (int i = 0; i < floatData.size(); i++) {
            data[i] = (int) (floatData.get(i) * maxMul);
          }
        }

        if (data.length == 0) {
          continue;
        }

        ByteBuffer inputBuffer = ByteBuffer.allocate(data.length * Integer.BYTES);
        for (int value : data) {
          inputBuffer.putInt(value);
        }
        byte[] inputBytes = inputBuffer.array();

        for (CompressionBenchmark benchmark : benchmarks) {
          ICompressor compressor = benchmark.compressorSupplier.get();
          IUnCompressor unCompressor = benchmark.unCompressorSupplier.get();

          byte[] compressed = new byte[0];
          long start = System.nanoTime();
          for (int repeat = 0; repeat < REPEAT_TIME; repeat++) {
            compressed = compressor.compress(inputBytes);
          }
          long end = System.nanoTime();
          long encodeTime = (end - start) / REPEAT_TIME;

          byte[] restored = new byte[inputBytes.length];
          start = System.nanoTime();
          for (int repeat = 0; repeat < REPEAT_TIME; repeat++) {
            restored = new byte[inputBytes.length];
            int uncompressedSize =
                unCompressor.uncompress(compressed, 0, compressed.length, restored, 0);
            Assert.assertEquals(inputBytes.length, uncompressedSize);
          }
          end = System.nanoTime();
          long decodeTime = (end - start) / REPEAT_TIME;

          Assert.assertArrayEquals(inputBytes, restored);
          BenchmarkResult result =
              new BenchmarkResult(
                  encodeTime,
                  decodeTime,
                  compressed.length,
                  compressed.length / (double) inputBytes.length);

          String[] record = {
            datasetName,
            benchmark.algorithm,
            String.valueOf(result.encodeTime),
            String.valueOf(result.decodeTime),
            String.valueOf(data.length),
            String.valueOf(inputBytes.length),
            String.valueOf(result.compressedSize),
            String.valueOf(result.compressionRatio)
          };
          benchmark.writer.writeRecord(record);
        }
      }
    } finally {
      for (CompressionBenchmark benchmark : benchmarks) {
        if (benchmark.writer != null) {
          benchmark.writer.close();
        }
      }
    }
  }
}