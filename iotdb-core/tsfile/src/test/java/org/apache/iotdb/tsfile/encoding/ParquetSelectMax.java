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
import java.util.HashMap;
import java.util.List;

// 把下面的方法放到与你的 main 同一个类里（作为 static 方法），或放入一个工具类并在 main 中调用。


public class ParquetSelectMax {

//    // -------------------------
//    // 辅助函数
//    // -------------------------
//    public static int popcount(long x) { return Long.bitCount(x); }
//
//    public static long pext64(long src, long mask) {
//        return Long.bitCount(mask) == 0 ? 0 : // 添加边界检查
//                Long.bitCount(mask) == 64 ? src : // 全掩码优化
//                        Long.bitCount(mask) == 1 ? (src & mask) != 0 ? 1 : 0 : // 单比特掩码优化
//                                pext64Impl(src, mask); // 原始实现
//    }
//
//    private static long pext64Impl(long src, long mask) {
//        long out = 0L;
//        long outPos = 0L;
//        long m = mask;
//        while (m != 0L) {
//            long lowest = m & -m;
//            int bitIndex = Long.numberOfTrailingZeros(lowest);
//            long bit = (src >>> bitIndex) & 1L;
//            out |= (bit << outPos);
//            outPos++;
//            m &= m - 1;
//        }
//        return out;
//    }
//
//    public static long pdep64(long src, long mask) {
//        return Long.bitCount(mask) == 0 ? 0 : // 添加边界检查
//                Long.bitCount(mask) == 64 ? src : // 全掩码优化
//                        Long.bitCount(mask) == 1 ? (src & 1) != 0 ? mask : 0 : // 单比特掩码优化
//                                pdep64Impl(src, mask); // 原始实现
//    }
//
//    private static long pdep64Impl(long src, long mask) {
//        long out = 0L;
//        long m = mask;
//        long srcPos = 0L;
//        while (m != 0L) {
//            long lowest = m & -m;
//            int bitIndex = Long.numberOfTrailingZeros(lowest);
//            long bit = (src >>> srcPos) & 1L;
//            if (bit != 0L) out |= (1L << bitIndex);
//            srcPos++;
//            m &= m - 1;
//        }
//        return out;
//    }
//
//    public static long extend64(long bitmap, long mask) {
//        long low = pdep64(bitmap, mask);
//        long high = pdep64(bitmap, mask - 1L);
//        return high - low;
//    }
//
//    public static long selectWord64(long valuesWord, long bitmap, long mask) {
//        long extended = extend64(bitmap, mask);
//        return pext64(valuesWord, extended);
//    }
//
//    // -------------------------
//    // 优化的按块打包函数
//    // -------------------------

    /** 计算最小能表示 range 所需的 bit 数 */
    public static int neededBitsForRange(long range) {
        if (range <= 0) return 1;
        return 64 - Long.numberOfLeadingZeros(range);
    }

//    /**
//     * Pack integer array into blocks of long[] words using k bits per value.
//     * Each block contains up to blockSize values.
//     */
//    public static long[][] packToBlocks(int[] values, int k, int blockSize) {
//        if (k <= 0 || k > 32) k = 32;
//        int fieldsPerWord = 64 / k;
//        int wordsPerBlock = (blockSize + fieldsPerWord - 1) / fieldsPerWord;
//        int numBlocks = (values.length + blockSize - 1) / blockSize;
//
//        long[][] blocks = new long[numBlocks][];
//        long mask = (k == 64) ? ~0L : ((1L << k) - 1L);
//
//        for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
//            int blockStart = blockIdx * blockSize;
//            int blockEnd = Math.min(blockStart + blockSize, values.length);
//            int blockValues = blockEnd - blockStart;
//
//            long[] blockWords = new long[wordsPerBlock];
//
//            for (int i = 0; i < blockValues; i++) {
//                int globalIndex = blockStart + i;
//                int widx = i / fieldsPerWord;
//                int pos = (i % fieldsPerWord) * k;
//                long v = ((long) values[globalIndex]) & mask;
//                blockWords[widx] |= (v << pos);
//            }
//
//            blocks[blockIdx] = blockWords;
//        }
//
//        return blocks;
//    }
//
//    // compute maskHigh: highest bit position for each k-bit field (used to PEXT MSB)
//    public static long computeMaskHigh(int k) {
//        if (k <= 0 || k > 64) return 0;
//        long m = 0L;
//        int fields = 64 / k;
//        for (int i = 0; i < fields; i++) {
//            int pos = i * k + (k - 1);
//            if (pos < 64) {
//                m |= (1L << pos);
//            }
//        }
//        return m;
//    }
//
//    // -------------------------
//    // 优化的查询函数，使用直接位操作而不是selectWord64
//    // -------------------------
//    public static int[] queryGreaterThanFromBlocks(long[][] blocks, int totalValues, int k, int offset, int lowerBound, int blockSize) {
//        if (k <= 0) throw new IllegalArgumentException("k must be > 0");
//        int fieldsPerWord = 64 / k;
//        long fieldMask = (k >= 64) ? ~0L : ((1L << k) - 1L);
//
//        // 预计算每个块中的值数量
//        int numBlocks = blocks.length;
//        int[] valuesPerBlock = new int[numBlocks];
//        for (int i = 0; i < numBlocks - 1; i++) {
//            valuesPerBlock[i] = blockSize;
//        }
//        valuesPerBlock[numBlocks - 1] = totalValues - (numBlocks - 1) * blockSize;
//
//        // 使用更高效的直接位操作而不是selectWord64
//        int[] temp = new int[totalValues];
//        int outLen = 0;
//
//        for (int blockIdx = 0; blockIdx < numBlocks; blockIdx++) {
//            long[] block = blocks[blockIdx];
//            int valuesInBlock = valuesPerBlock[blockIdx];
//            int wordsInBlock = (valuesInBlock + fieldsPerWord - 1) / fieldsPerWord;
//
//            for (int widx = 0; widx < wordsInBlock; widx++) {
//                long word = block[widx];
//
//                // 直接提取字段而不是使用selectWord64
//                for (int b = 0; b < fieldsPerWord; b++) {
//                    int localIndex = widx * fieldsPerWord + b;
//                    if (localIndex >= valuesInBlock) break;
//
//                    int globalIndex = blockIdx * blockSize + localIndex;
//                    // 使用直接位移和掩码操作提取值
//                    int val = (int) ((word >>> (b * k)) & fieldMask);
//                    int actual = val + offset;
//                    if (actual > lowerBound) {
//                        temp[outLen++] = globalIndex;
//                    }
//                }
//            }
//        }
//
//        // 只返回实际需要的部分
//        if (outLen == totalValues) {
//            return temp; // 所有值都满足条件
//        }
//
//        int[] res = new int[outLen];
//        System.arraycopy(temp, 0, res, 0, outLen);
//        return res;
//    }
//
    // -------------------------
    // 辅助 I/O 函数
    // -------------------------
    public static int getDecimalPrecision(String str) {
        int decimalIndex = str.indexOf(".");
        if (decimalIndex == -1) return 0;
        return str.substring(decimalIndex + 1).length();
    }

    public static String extractFileName(String path) {
        if (path == null || path.isEmpty()) return "";
        File file = new File(path);
        String fileName = file.getName();
        int dotIndex = fileName.lastIndexOf('.');
        if (dotIndex == -1 || dotIndex == 0) return fileName;
        return fileName.substring(0, dotIndex);
    }



    public static long[][] packToBlocks(int[] shifted, int k, int blockSize) {
        int n = shifted.length;
        int numBlocks = (n + blockSize - 1) / blockSize;
        long[][] blocks = new long[numBlocks][];
        for (int b = 0; b < numBlocks; b++) {
            int startIdx = b * blockSize;
            int blockCount = Math.min(blockSize, n - startIdx);
            long bits = (long) blockCount * k;
            int words = (int) ((bits + 63) / 64);
            long[] buf = new long[words];
            // 按值逐位写入（little-endian bit packing，每个 value 的最低位放在更低的 bit）
            for (int i = 0; i < blockCount; i++) {
                int value = shifted[startIdx + i];
                long bitPos = (long) i * k;
                for (int bit = 0; bit < k; bit++) {
                    int bval = (value >>> bit) & 1;
                    if (bval != 0) {
                        setBit(buf, bitPos + bit, 1);
                    } // 若为0可跳过（buf 默认 0）
                }
            }
            blocks[b] = buf;
        }
        return blocks;
    }

//    public static int[] queryGreaterThanFromBlocks(long[][] packedBlocks, int n, int k, int min, int lower, int blockSize) {
//        List<Integer> hits = new ArrayList<>();
//        int numBlocks = packedBlocks.length;
//        for (int b = 0; b < numBlocks; b++) {
//            long[] block = packedBlocks[b];
//            int startIdx = b * blockSize;
//            int blockCount = Math.min(blockSize, n - startIdx);
//            for (int i = 0; i < blockCount; i++) {
//                long val = extractKbitValue(block, i, k); // shifted value
//                long original = val + (long) min;
//                if (original > lower) {
//                    hits.add(startIdx + i);
//                }
//            }
//        }
//        // 转为 int[]
//        int[] out = new int[hits.size()];
//        for (int i = 0; i < hits.size(); i++) out[i] = hits.get(i);
//        return out;
//    }
public static long calculateSumFromBlocks(long[][] packedBlocks, int n, int k, int min, int blockSize) {
    long sum = 0L;
    int numBlocks = packedBlocks.length;

    for (int b = 0; b < numBlocks; b++) {
        long[] block = packedBlocks[b];
        int blockCount = Math.min(blockSize, n - b * blockSize);

        for (int i = 0; i < blockCount; i++) {
            long val = extractKbitValue(block, i, k); // 提取压缩值
            int original = (int) (val + min);         // 转换为原始值
            sum += original;                          // 累加到总和
        }
    }

    return sum;
}
    public static int findMaxFromBlocks(long[][] packedBlocks, int n, int k, int min, int blockSize) {
        int maxVal = Integer.MIN_VALUE;
        int numBlocks = packedBlocks.length;

        for (int b = 0; b < numBlocks; b++) {
            long[] block = packedBlocks[b];
            int blockCount = Math.min(blockSize, n - b * blockSize);

            for (int i = 0; i < blockCount; i++) {
                long val = extractKbitValue(block, i, k); // 提取压缩值
                int original = (int) (val + min);         // 转换为原始值
                if (original > maxVal) {
                    maxVal = original;
                }
            }
        }

        return maxVal;
    }

    /* ------- 辅助位操作（逐位最慢实现） ------- */

    private static int getBit(long[] words, long bitIndex) {
        int w = (int) (bitIndex >>> 6); // /64
        int off = (int) (bitIndex & 63L);
        return (int) ((words[w] >>> off) & 1L);
    }

    private static void setBit(long[] words, long bitIndex, int v) {
        int w = (int) (bitIndex >>> 6);
        int off = (int) (bitIndex & 63L);
        if (v == 1) {
            words[w] |= (1L << off);
        } else {
            words[w] &= ~(1L << off);
        }
    }

    // 从单个 block 的 bit-packed long[] 中提取第 idx 个 k-bit 值（little-endian packing）
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
    public static int countFromBlocks(long[][] packedBlocks, int k, int blockSize) {
        int totalCount = 0;

        for (long[] block : packedBlocks) {
            // 计算每个块中的值数量
            // 每个块最多有 blockSize 个值，但最后一个块可能不满
            // 我们可以通过块的总位数除以每个值的位数来计算实际值数量
            long totalBitsInBlock = (long) block.length * 64L;
            int valuesInBlock = (int) (totalBitsInBlock / k);

            // 确保不超过块大小
            valuesInBlock = Math.min(valuesInBlock, blockSize);

            totalCount += valuesInBlock;
        }

        return totalCount;
    }
    // -------------------------
    // 优化的 main 函数
    // -------------------------
    @Test
    public void maxTest() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String input_parent_dir = parent_dir + "dataset/";
        String output_parent_dir = parent_dir + "result/query_parquetproto/";

        int block_size = 512;

        HashMap<String, Integer> queryRange = new HashMap<>();
        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 10);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);


        int repeatTime = 100;
        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        String outputPath = output_parent_dir + "parquetselect_query_max.csv";
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
        if (csvFiles == null) {
            System.err.println("No csv files under " + input_parent_dir);
            writer.close();
            return;
        }

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println("Dataset: " + datasetName);
            if(!queryRange.containsKey(datasetName)) continue;

            InputStream inputStream = Files.newInputStream(file.toPath());
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Float> data1 = new ArrayList<>();

            int max_decimal = 0;
            while (loader.readRecord()) {
                String f_str = loader.getValues()[0];
                if (f_str.isEmpty()) continue;
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal) max_decimal = cur_decimal;
                data1.add(Float.valueOf(f_str));
            }
            inputStream.close();

            int n = data1.size();
            int[] data2_arr = new int[n];
            int max_mul = (int) Math.pow(10, max_decimal);
            for (int i = 0; i < n; i++) {
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

            // 预热JVM
//            packToBlocks(shifted, k, block_size);
            findMaxFromBlocks(packToBlocks(shifted, k, block_size), n, k, min, block_size);

            // encoding benchmark: repeatedly pack
            long s = System.nanoTime();
//            for (int repeat = 0; repeat < repeatTime; repeat++) {
                packedBlocks = packToBlocks(shifted, k, block_size);
//            }
            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);

            // 计算压缩大小
            for (long[] block : packedBlocks) {
                compressed_size += block.length * Long.BYTES;
            }

            double ratioTmp;
            if (integerDatasets.contains(datasetName)) {
                ratioTmp = compressed_size / (double) (n * Integer.BYTES);
            } else {
                ratioTmp = compressed_size / (double) (n * Long.BYTES);
            }

            System.out.println("Querying...");

            int lower = queryRange.getOrDefault(datasetName, 0);
            s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                int hits = findMaxFromBlocks(packedBlocks, n, k, min, block_size);
                // hits not used further here, just to simulate query work
            }
            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "ParquetSelect-proto",
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(n),
                    String.valueOf((long) compressed_size),
                    String.valueOf(ratioTmp)
            };
            writer.writeRecord(record);

            System.out.println("k (bits): " + k + " compressed bytes: " + (long) compressed_size + " ratio: " + ratioTmp);
        }

        writer.close();
        System.out.println("Done. Results written to " + outputPath);
    }


    @Test
    public void sumTest() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String input_parent_dir = parent_dir + "dataset/";
        String output_parent_dir = parent_dir + "result/query_parquetproto/";

        int block_size = 512;

        HashMap<String, Integer> queryRange = new HashMap<>();
        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 10);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);


        int repeatTime = 200;
        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        String outputPath = output_parent_dir + "parquetselect_query_sum.csv";
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
        if (csvFiles == null) {
            System.err.println("No csv files under " + input_parent_dir);
            writer.close();
            return;
        }

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println("Dataset: " + datasetName);
            if(!queryRange.containsKey(datasetName)) continue;

            InputStream inputStream = Files.newInputStream(file.toPath());
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Float> data1 = new ArrayList<>();

            int max_decimal = 0;
            while (loader.readRecord()) {
                String f_str = loader.getValues()[0];
                if (f_str.isEmpty()) continue;
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal) max_decimal = cur_decimal;
                data1.add(Float.valueOf(f_str));
            }
            inputStream.close();

            int n = data1.size();
            int[] data2_arr = new int[n];
            int max_mul = (int) Math.pow(10, max_decimal);
            for (int i = 0; i < n; i++) {
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

            // 预热JVM
//            packToBlocks(shifted, k, block_size);
            calculateSumFromBlocks(packToBlocks(shifted, k, block_size), n, k, min, block_size);

            // encoding benchmark: repeatedly pack
            long s = System.nanoTime();
//            for (int repeat = 0; repeat < repeatTime; repeat++) {
            packedBlocks = packToBlocks(shifted, k, block_size);
//            }
            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);

            // 计算压缩大小
            for (long[] block : packedBlocks) {
                compressed_size += block.length * Long.BYTES;
            }

            double ratioTmp;
            if (integerDatasets.contains(datasetName)) {
                ratioTmp = compressed_size / (double) (n * Integer.BYTES);
            } else {
                ratioTmp = compressed_size / (double) (n * Long.BYTES);
            }

            System.out.println("Querying...");

            int lower = queryRange.getOrDefault(datasetName, 0);
            s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
                calculateSumFromBlocks(packedBlocks, n, k, min, block_size);
                // hits not used further here, just to simulate query work
            }
            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "ParquetSelect-proto",
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(n),
                    String.valueOf((long) compressed_size),
                    String.valueOf(ratioTmp)
            };
            writer.writeRecord(record);

            System.out.println("k (bits): " + k + " compressed bytes: " + (long) compressed_size + " ratio: " + ratioTmp);
        }

        writer.close();
        System.out.println("Done. Results written to " + outputPath);
    }

    @Test
    public void countTest() throws IOException {
        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String input_parent_dir = parent_dir + "dataset/";
        String output_parent_dir = parent_dir + "result/query_parquetproto/";

        int block_size = 512;

        HashMap<String, Integer> queryRange = new HashMap<>();
        queryRange.put("Bird-migration", 2500000);
        queryRange.put("Bitcoin-price", 160000000);
        queryRange.put("City-temp", 480);
        queryRange.put("Dewpoint-temp", 9500);
        queryRange.put("IR-bio-temp", -300);
        queryRange.put("PM10-dust", 1000);
        queryRange.put("Stocks-DE", 40000);
        queryRange.put("Stocks-UK", 20000);
        queryRange.put("Stocks-USA", 5000);
        queryRange.put("Wind-Speed", 50);
        queryRange.put("Wine-Tasting", 10);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);


        int repeatTime = 200;
        List<String> integerDatasets = new ArrayList<>();
        integerDatasets.add("Wine-Tasting");

        String outputPath = output_parent_dir + "parquetselect_query_count.csv";
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
        if (csvFiles == null) {
            System.err.println("No csv files under " + input_parent_dir);
            writer.close();
            return;
        }

        for (File file : csvFiles) {
            String datasetName = extractFileName(file.toString());
            System.out.println("Dataset: " + datasetName);
            if(!queryRange.containsKey(datasetName)) continue;

            InputStream inputStream = Files.newInputStream(file.toPath());
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Float> data1 = new ArrayList<>();

            int max_decimal = 0;
            while (loader.readRecord()) {
                String f_str = loader.getValues()[0];
                if (f_str.isEmpty()) continue;
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal) max_decimal = cur_decimal;
                data1.add(Float.valueOf(f_str));
            }
            inputStream.close();

            int n = data1.size();
            int[] data2_arr = new int[n];
            int max_mul = (int) Math.pow(10, max_decimal);
            for (int i = 0; i < n; i++) {
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

            // 预热JVM
//            packToBlocks(shifted, k, block_size);
            calculateSumFromBlocks(packToBlocks(shifted, k, block_size), n, k, min, block_size);

            // encoding benchmark: repeatedly pack
            long s = System.nanoTime();
//            for (int repeat = 0; repeat < repeatTime; repeat++) {
            packedBlocks = packToBlocks(shifted, k, block_size);
//            }
            long e = System.nanoTime();
            encodeTime += ((e - s) / repeatTime);

            // 计算压缩大小
            for (long[] block : packedBlocks) {
                compressed_size += block.length * Long.BYTES;
            }

            double ratioTmp;
            if (integerDatasets.contains(datasetName)) {
                ratioTmp = compressed_size / (double) (n * Integer.BYTES);
            } else {
                ratioTmp = compressed_size / (double) (n * Long.BYTES);
            }

            System.out.println("Querying...");

            int lower = queryRange.getOrDefault(datasetName, 0);
            s = System.nanoTime();
            for (int repeat = 0; repeat < repeatTime; repeat++) {
               int hits = countFromBlocks( packedBlocks,k, block_size);
                // hits not used further here, just to simulate query work
            }
            e = System.nanoTime();
            decodeTime += ((e - s) / repeatTime);

            String[] record = {
                    datasetName,
                    "ParquetSelect-proto",
                    String.valueOf(encodeTime),
                    String.valueOf(decodeTime),
                    String.valueOf(n),
                    String.valueOf((long) compressed_size),
                    String.valueOf(ratioTmp)
            };
            writer.writeRecord(record);

            System.out.println("k (bits): " + k + " compressed bytes: " + (long) compressed_size + " ratio: " + ratioTmp);
        }

        writer.close();
        System.out.println("Done. Results written to " + outputPath);
    }
}