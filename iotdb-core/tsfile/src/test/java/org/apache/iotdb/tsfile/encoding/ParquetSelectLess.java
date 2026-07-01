package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

public class ParquetSelectLess {

    // -----------------------
    // 软件 PEXT / PDEP 实现
    // -----------------------
    public static long pext64(long src, long mask) {
        long out = 0L;
        long outPos = 0L;
        long m = mask;
        while (m != 0L) {
            long lowest = m & -m;
            int bitIndex = Long.numberOfTrailingZeros(lowest);
            long bit = (src >>> bitIndex) & 1L;
            out |= (bit << outPos);
            outPos++;
            m &= m - 1;
        }
        return out;
    }

    public static long pdep64(long src, long mask) {
        long out = 0L;
        long m = mask;
        long srcPos = 0L;
        while (m != 0L) {
            long lowest = m & -m;
            int bitIndex = Long.numberOfTrailingZeros(lowest);
            long bit = (src >>> srcPos) & 1L;
            if (bit != 0L) out |= (1L << bitIndex);
            srcPos++;
            m &= m - 1;
        }
        return out;
    }

    public static long extend64(long bitmap, long mask) {
        long low = pdep64(bitmap, mask);
        long high = pdep64(bitmap, mask - 1L);
        return high - low;
    }

    public static long selectWord64(long valuesWord, long bitmap, long mask) {
        long extended = extend64(bitmap, mask);
        return pext64(valuesWord, extended);
    }

    public static long computeMaskHigh(int k) {
        long m = 0L;
        for (int i = 0; i * k + (k - 1) < 64; i++) {
            int pos = i * k + (k - 1);
            m |= (1L << pos);
        }
        return m;
    }

    // -----------------------
    // 按块处理的 less than 查询实现
    // -----------------------

    /**
     * Pack integer array into blocks of long[] words using k bits per value.
     * Each block contains up to blockSize values.
     * Returns a 2D array where first dimension is block index and second is words in block.
     */
    public static long[][] packToBlocks(int[] values, int k, int blockSize) {
        if (k <= 0 || k > 32) k = 32;
        int fieldsPerWord = 64 / k;
        int wordsPerBlock = (blockSize + fieldsPerWord - 1) / fieldsPerWord;
        int numBlocks = (values.length + blockSize - 1) / blockSize;

        long[][] blocks = new long[numBlocks][wordsPerBlock];
        long mask = (k == 64) ? ~0L : ((1L << k) - 1L);

        for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
            int blockStart = blockIdx * blockSize;
            int blockEnd = Math.min(blockStart + blockSize, values.length);
            int blockValues = blockEnd - blockStart;

            for (int i = 0; i < blockValues; i++) {
                int globalIndex = blockStart + i;
                int widx = i / fieldsPerWord;
                int pos = (i % fieldsPerWord) * k;
                long v = ((long) values[globalIndex]) & mask;
                blocks[blockIdx][widx] |= (v << pos);
            }
        }

        return blocks;
    }

    /**
     * Query less than from packed blocks
     * 优化版本：使用直接位操作而不是 selectWord64 提高性能
     */
    public static int[] queryLessThanFromBlocks(
            long[][] packedBlocks,
            int n,
            int k,
            int min,
            int upper,          // 传入比较上限
            int blockSize) {

        List<Integer> hits = new ArrayList<>();
        int numBlocks = packedBlocks.length;

        for (int b = 0; b < numBlocks; b++) {
            long[] block = packedBlocks[b];
            int startIdx = b * blockSize;
            int blockCount = Math.min(blockSize, n - startIdx);

            for (int i = 0; i < blockCount; i++) {
                long val = extractKbitValue(block, i, k); // shifted value
                long original = val + (long) min;
                // 关键改动：小于比较
                if (original < upper) {
                    hits.add(startIdx + i);
                }
            }
        }

        // 转为 int[]
        int[] out = new int[hits.size()];
        for (int i = 0; i < hits.size(); i++) {
            out[i] = hits.get(i);
        }
        return out;
    }
    private static long extractKbitValue(long[] valuesWords, int idx, int k) {
        long bitPos = (long) idx * (long) k;
        int w = (int) (bitPos >>> 6);
        int off = (int) (bitPos & 63L);
        if (off + k <= 64) {
            long word = valuesWords[w];
            long mask = (k == 64) ? ~0L : ((1L << k) - 1L);
            return (word >>> off) & mask;
        } else {
            // 跨 word 边界
            int lowBits = 64 - off;
            long lowMask = (lowBits == 64) ? ~0L : ((1L << lowBits) - 1L);
            long low = (valuesWords[w] >>> off) & lowMask;
            long high = valuesWords[w + 1] & ((1L << (k - lowBits)) - 1L);
            return (high << lowBits) | low;
        }
    }

    // -----------------------
    // 工具函数
    // -----------------------
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

    public static int neededBitsForRange(long range) {
        if (range <= 0) return 1;
        return 64 - Long.numberOfLeadingZeros(range);
    }

    // -----------------------
    // testQuery()：修改为使用二维数组
    // -----------------------
    @Test
    public void testQuery() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String input_parent_dir = parent_dir + "dataset/";
        String output_parent_dir = parent_dir + "result/query_parquetproto/";

        int block_size = 512;

        HashMap<String, Integer> queryRange = new HashMap<>();
        queryRange.put("Bird-migration", 2600000);
        queryRange.put("Bitcoin-price", 170000000);
        queryRange.put("City-temp", 700);
        queryRange.put("Dewpoint-temp", 9600);
        queryRange.put("IR-bio-temp", -200);
        queryRange.put("PM10-dust", 2000);
        queryRange.put("Stocks-DE", 90000);
        queryRange.put("Stocks-UK", 30000);
        queryRange.put("Stocks-USA", 6000);
        queryRange.put("Wind-Speed", 60);
        queryRange.put("Wine-Tasting", 10);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        int repeatTime = 200;
        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        String outputPath = output_parent_dir + "parquetselect_query_less.csv";

        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');

        String[] head = {
                "Dataset",
                "Encoding Algorithm",
                "Encoding Time",
                "Decoding Time",
                "Points",
                "Compressed Size",
                "Compression Ratio"
        };
        writer.writeRecord(head);

        File directory = new File(input_parent_dir);
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);
            if(!queryRange.containsKey(datasetName))
                continue;

            InputStream inputStream = Files.newInputStream(file.toPath());
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Float> data1 = new ArrayList<>();

            int max_decimal = 0;
            while (loader.readRecord()) {
                String f_str = loader.getValues()[0];
                if (f_str.isEmpty()) {
                    continue;
                }
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal)
                    max_decimal = cur_decimal;
                data1.add(Float.valueOf(f_str));
            }
            inputStream.close();

            int n = data1.size();
            int[] data2_arr = new int[n];
            int max_mul = (int) Math.pow(10, max_decimal);
            for (int i = 0; i < data1.size(); i++) {
                data2_arr[i] = (int) (data1.get(i) * max_mul);
            }

            // compute min/max and needed bitwidth
            int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
            for (int v : data2_arr) {
                if (v < min) min = v;
                if (v > max) max = v;
            }
            long range = (long) max - (long) min;
            int k = neededBitsForRange(range);
            if (k < 1) k = 1;
            if (k > 32) k = 32;

            // pack values (subtract min to make non-negative)
            int[] shifted = new int[n];
            for (int i = 0; i < n; i++) shifted[i] = data2_arr[i] - min;

            long[][] packedBlocks = null;
            long encodeTime = 0;
            long decodeTime = 0;
            double compressed_size = 0;

            // Encoding benchmark
            long s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                packedBlocks = packToBlocks(shifted, k, block_size);
            }
            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);

            // Calculate compressed size
            for (long[] block : packedBlocks) {
                compressed_size += block.length * Long.BYTES;
            }

            double ratioTmp;
            if (integerDatasets.contains(datasetName)) {
                ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
            } else {
                ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
            }

            System.out.println("Query");

            s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                int upper = queryRange.getOrDefault(datasetName, 0);
                int[] hits = queryLessThanFromBlocks(packedBlocks, n, k, min, upper, block_size);
            }
            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "ParquetSelect-proto",
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(data1.size()),
                    String.valueOf((long) compressed_size),
                    String.valueOf(ratioTmp)
            };
            writer.writeRecord(record);

            System.out.println("Compression ratio: " + ratioTmp);
        }

        writer.close();
    }
}