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
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SubcolumnLongMaterializeR1D3 {

    public static void QueryTwoColumns(byte[] encoded_result1, byte[] encoded_result2, long upper_bound1,
            long upper_bound2) {
        int[] first_column_results = new int[encoded_result1.length];
        int[] first_result_length = new int[1];

        Query(encoded_result1, upper_bound1, first_column_results, first_result_length);

        long[] final_results = new long[first_result_length[0]];
        int[] final_result_length = new int[1];

        QueryWithIndices(encoded_result2, upper_bound2, first_column_results, first_result_length[0],
                final_results, final_result_length);
    }

    public static void QueryWithIndices(byte[] encoded_result, long upper_bound,
                                        int[] candidate_indices, int candidate_length,
                                        long[] result, int[] result_length) {
        int encode_pos = 0;

        int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16)
                | ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        // 初始化结果索引
        result_length[0] = 0;

        int[] blockIndicesCount = new int[num_blocks + 1];

        for (int i = 0; i < candidate_length; i++) {
            int index = candidate_indices[i];
            int blockIndex = index / block_size;
            blockIndicesCount[blockIndex]++;
        }

        int[][] blockIndices = new int[num_blocks + 1][];
        for (int i = 0; i <= num_blocks; i++) {
            blockIndices[i] = new int[blockIndicesCount[i]];
        }

        int[] currentIndices = new int[num_blocks + 1];

        for (int i = 0; i < candidate_length; i++) {
            int index = candidate_indices[i];
            int blockIndex = index / block_size;
            int localIndex = index % block_size;

            blockIndices[blockIndex][currentIndices[blockIndex]] = localIndex;
            currentIndices[blockIndex]++;
        }

        // 遍历所有块
        for (int i = 0; i < num_blocks; i++) {

            if (blockIndicesCount[i] == 0) {
                // 计算跳过此块所需的字节数
                encode_pos = SkipBlock(encoded_result, i, block_size,
                        block_size, encode_pos);
                continue;
            }

            // 对该块中的候选索引执行查询
            encode_pos = BlockQueryWithIndices(encoded_result, i, block_size,
                    block_size, encode_pos, upper_bound,
                    blockIndices[i], blockIndicesCount[i], result, result_length);
        }

        int remainder = data_length % block_size;

        if (remainder > 0) {
            if (blockIndicesCount[num_blocks] > 0) {
                if (remainder <= 3) {
                    for (int j = 0; j < blockIndicesCount[num_blocks]; j++) {
                        int idx = blockIndices[num_blocks][j];
                        int offset = num_blocks * block_size + idx;
                        if (offset < data_length) {
                            int value = ((encoded_result[encode_pos + idx * 4] & 0xFF) << 24) |
                                    ((encoded_result[encode_pos + idx * 4 + 1] & 0xFF) << 16) |
                                    ((encoded_result[encode_pos + idx * 4 + 2] & 0xFF) << 8) |
                                    (encoded_result[encode_pos + idx * 4 + 3] & 0xFF);
                            if (value < upper_bound) {
                                result[result_length[0]] = offset;
                                result_length[0]++;
                            }
                        }
                    }
                    encode_pos += remainder * 4;
                } else {

                    encode_pos = BlockQueryWithIndices(encoded_result, num_blocks, block_size,
                            remainder, encode_pos, upper_bound,
                            blockIndices[num_blocks], blockIndicesCount[num_blocks], result, result_length);
                }
            } else {
                // 没有候选索引，跳过剩余部分
                if (remainder <= 3) {
                    encode_pos += remainder * 4;
                } else {
                    encode_pos = SkipBlock(encoded_result, num_blocks, block_size,
                            remainder, encode_pos);
                }
            }
        }
    }
    public static long bytes2Long(byte[] encoded, int start, int num) {
        long value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static int BlockQueryWithIndices(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, long upper_bound, int[] candidate_indices, int candidate_length,
            long[] result, int[] result_length) {
        long[] min_delta = new long[3];

        min_delta[0] = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        int m = encoded_result[encode_pos];
        encode_pos += 1;

        upper_bound -= min_delta[0];

        // 所有索引默认都是候选索引
        int[] filtered_indices = new int[candidate_length];
        int filtered_length = candidate_length;
        System.arraycopy(candidate_indices, 0, filtered_indices, 0, candidate_length);

        if (m == 0) {
            if (upper_bound > 0) {
                for (int i = 0; i < filtered_length; i++) {
                    result[result_length[0]] = block_size * block_index + filtered_indices[i];
                    result_length[0]++;
                }
            }
            return encode_pos;
        }

        int bw = SubcolumnTest.bitWidth(block_size);

        int beta = encoded_result[encode_pos];
        encode_pos += 1;

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = SubcolumnLongTest.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        int[][] subcolumnList = new int[l][remainder];

        int[] encodingType = new int[l];

        encode_pos = SubcolumnLongTest.decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) {
                if (upper_bound <= 0) {
                    encode_pos *= 8;
                    encode_pos += bitWidthList[i] * remainder;
                    encode_pos = (encode_pos + 7) / 8;
                    continue;
                }

                encode_pos *= 8;

                int new_length = 0;
                for (int j = 0; j < filtered_length; j++) {
                    int index = filtered_indices[j];

                    subcolumnList[i][index] = SubcolumnTest.bytesToInt(encoded_result,
                            encode_pos + index * bitWidthList[i], bitWidthList[i]);
                    long value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);
                    if (subcolumnList[i][index] < value) {
                        result[result_length[0]] = block_size * block_index + index;
                        result_length[0]++;
                    } else if (subcolumnList[i][index] == value) {
                        filtered_indices[new_length] = index;
                        new_length++;
                    }
                }

                filtered_length = new_length;

                encode_pos += remainder * bitWidthList[i];
                encode_pos = (encode_pos + 7) / 8;

            } else {
                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);

                encode_pos += 2;

                if (upper_bound <= 0) {
                    encode_pos *= 8;
                    encode_pos += bw * index;
                    encode_pos = (encode_pos + 7) / 8;

                    encode_pos *= 8;
                    encode_pos += bitWidthList[i] * index;
                    encode_pos = (encode_pos + 7) / 8;
                    continue;
                }

                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, bw, index, run_length);
                encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, bitWidthList[i], index,
                        rle_values);

                int new_length = 0;
                long value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);

                // 为每个候选索引查找对应的RLE值
                for (int j = 0; j < filtered_length; j++) {
                    int index_candidate = filtered_indices[j];

                    // 查找包含此索引的RLE段
                    int rleIndex = 0;
                    int currentPos = 0;

                    while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index) {
                        if (rle_values[rleIndex] < value) {
                            result[result_length[0]] = block_size * block_index + index_candidate;
                            result_length[0]++;
                        } else if (rle_values[rleIndex] == value) {
                            filtered_indices[new_length] = index_candidate;
                            new_length++;
                        }
                    }
                }

                filtered_length = new_length;
            }
        }

        return encode_pos;
    }

    private static int SkipBlock(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos) {
        // int[] min_delta = new int[3];

        encode_pos += 8;

        int m = encoded_result[encode_pos];
        encode_pos += 1;

        if (m == 0) {
            return encode_pos;
        }

        int bw = SubcolumnTest.bitWidth(block_size);

        int beta = encoded_result[encode_pos];
        encode_pos += 1;


//        System.out.println("beta:"+beta);
//        System.out.println("m:"+m);
//        System.out.println("m:"+(m + beta - 1) / beta);
        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        int[] encodingType = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];

            if (type == 0) {

                encode_pos = (encode_pos * 8 + bitWidthList[i] * remainder + 7) / 8;
            } else {
                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);
                encode_pos += 2;

                encode_pos = (encode_pos * 8 + bw * index + 7) / 8;
                encode_pos = (encode_pos * 8 + bitWidthList[i] * index + 7) / 8;
            }
        }

        return encode_pos;
    }

    public static void Query(byte[] encoded_result, long upper_bound, int[] result, int[] result_length) {

        int encode_pos = 0;

        int data_length = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16)
                |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int block_size = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        // 查询结果
        // int[] result = new int[data_length];
        // int[] result_length = new int[1];

        result_length[0] = 0;

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockQueryIndex(encoded_result, i, block_size,
                    block_size, encode_pos, upper_bound,
                    result, result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                long value =  bytes2Long(encoded_result, encode_pos, 8);
                encode_pos += 8;
                if (value < upper_bound) {
                    result[result_length[0]] =  data_length / block_size * block_size +i;
                    result_length[0]++;
                }
            }
        } else {
            encode_pos = BlockQueryIndex(encoded_result, num_blocks, block_size,
                    remainder, encode_pos, upper_bound,
                    result, result_length);
        }

    }

    public static int BlockQueryIndex(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, long upper_bound, int[] result, int[] result_length) {
        long[] min_delta = new long[3];

        min_delta[0] = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        // int[] block_data = new int[remainder];

        int m = encoded_result[encode_pos];
        encode_pos += 1;


        upper_bound -= min_delta[0];

        // 候选索引列表，当前分列值和 upper_bound 相应值相等的索引
        int[] candidate_indices = new int[remainder];
        int candidate_length = 0;
        for (int i = 0; i < remainder; i++) {
            candidate_indices[i] = i;
            candidate_length++;
        }

        if (m == 0) {
            if (upper_bound > 0) {
                for (int i = 0; i < remainder; i++) {
                    result[result_length[0]] = block_size * block_index + i;
                    result_length[0]++;
                }
            }
            return encode_pos;
        }

        int bw = SubcolumnTest.bitWidth(block_size);

        int beta = encoded_result[encode_pos];
        encode_pos += 1;



        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        long[][] subcolumnList = new long[l][remainder];

        int[] encodingType = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) {

                if (upper_bound <= 0) {
                    encode_pos *= 8;
                    encode_pos += bitWidthList[i] * remainder;
                    encode_pos = (encode_pos + 7) / 8;
                    continue;
                }

                encode_pos *= 8;

                int new_length = 0;
                for (int j = 0; j < candidate_length; j++) {
                    int index = candidate_indices[j];

                    subcolumnList[i][index] = SubcolumnTest.bytesToInt(encoded_result,
                            encode_pos + index * bitWidthList[i], bitWidthList[i]);
                    long value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);
                    if (subcolumnList[i][index] < value) {
                        result[result_length[0]] = block_size * block_index + index;
                        result_length[0]++;
                    } else if (subcolumnList[i][index] == value) {
                        candidate_indices[new_length] = index;
                        new_length++;
                    }
                }

                candidate_length = new_length;

                encode_pos += remainder * bitWidthList[i];
                encode_pos = (encode_pos + 7) / 8;

            } else {

                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);

                encode_pos += 2;

                if (upper_bound <= 0) {
                    encode_pos *= 8;
                    encode_pos += bw * index;
                    encode_pos = (encode_pos + 7) / 8;

                    encode_pos *= 8;
                    encode_pos += bitWidthList[i] * index;
                    encode_pos = (encode_pos + 7) / 8;
                    continue;
                }

                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, bw, index, run_length);
                encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, bitWidthList[i], index,
                        rle_values);

                int new_length = 0;
                int rleIndex = 0;
                int currentPos = 0;
                long value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);

                for (int j = 0; j < candidate_length; j++) {
                    int index_candidate = candidate_indices[j];

                    while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index) {
                        if (rle_values[rleIndex] < value) {
                            result[result_length[0]] = block_size * block_index + index_candidate;
                            result_length[0]++;
                        } else if (rle_values[rleIndex] == value) {
                            candidate_indices[new_length] = index_candidate;
                            new_length++;
                        }
                    }
                }

                candidate_length = new_length;

            }
        }

        return encode_pos;
    }

    public static int getDecimalPrecision(String str) {
        // 查找小数点的位置
        int decimalIndex = str.indexOf(".");

        // 如果没有小数点，精度为0
        if (decimalIndex == -1) {
            return 0;
        }

        // 获取小数点后的部分并返回其长度
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



    static class DecodedBlock {
        long[] values;
        int newEncodePos;
        DecodedBlock(long[] values, int newEncodePos) { this.values = values; this.newEncodePos = newEncodePos; }
    }

    /**
     * 读 big-endian 32-bit int（与 Query/BlockQueryIndex 中的读取一致）。
     */
    private static int readInt32BE(byte[] arr, int pos) {
        return ((arr[pos] & 0xFF) << 24) | ((arr[pos + 1] & 0xFF) << 16) | ((arr[pos + 2] & 0xFF) << 8)
                | (arr[pos + 3] & 0xFF);
    }

    /**
     * 严格地完整解码一个块（反向实现 BlockQueryIndex/WithIndices 的解析逻辑）。
     *
     * @param encoded_result 整列的字节数组
     * @param block_index    块索引（仅用于可读性/日志，函数内部不用它定位）
     * @param block_size     标称块大小（用于计算 bw）
     * @param remainder      本块的元素数（最后一个块可能小于 block_size）
     * @param encode_pos     当前字节偏移（函数会从这里读取，并返回更新后的字节偏移）
     * @return DecodedBlock，包含本块每个位置的解码整型值（长度 = remainder）以及新的 encode_pos
     */
    public static DecodedBlock decodeBlockValues(byte[] encoded_result, int block_index, int block_size, int remainder,
                                                 int encode_pos) {
        // 1) 读取 min_delta
        long min_delta =bytes2Long(encoded_result, encode_pos, 8);;
        encode_pos += 8;

        // 2) 读取 m
        int m = encoded_result[encode_pos] & 0xFF;
        encode_pos += 1;

        // 如果 m == 0：表示无 subcolumns（所有值等于 min_delta）
        if (m == 0) {
            long[] vals = new long[remainder];
            if (remainder > 0) Arrays.fill(vals, min_delta);
            return new DecodedBlock(vals, encode_pos);
        }

        // 3) 其他元信息
        int bw = SubcolumnTest.bitWidth(block_size); // 基本宽度，用于 RLE run-length 的位宽
        int beta = encoded_result[encode_pos] & 0xFF;
        encode_pos += 1;

        int l = (m + beta - 1) / beta;

        // 4) 读取 bitWidthList（每个 subcolumn 的位宽，使用 decodeBitPacking(bits=8)）
        int[] bitWidthList = new int[l];
        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        // 5) 读取 encodingType（每个 subcolumn 的编码类型：0=bitpacked, 1=RLE）
        int[] encodingType = new int[l];
        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        // 6) 逐个 subcolumn（从高位 i=l-1 到 i=0）合成值
        long[] blockValues = new long[remainder];
        Arrays.fill(blockValues, 0);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            int bw_i = bitWidthList[i];

            if (type == 0) {
                // bitpacked: 在当前 encode_pos (字节偏移) 的位流上连续存 remainder 个 bw_i 位值
                int bitStart = encode_pos * 8; // 转为位偏移
                // 对每个位置抽取 bw_i 位并左移累加
                for (int p = 0; p < remainder; p++) {
                    int bitOffset = bitStart + p * bw_i;
                    int part = SubcolumnTest.bytesToInt(encoded_result, bitOffset, bw_i);
                    blockValues[p] |= (part << (i * beta));
                }
                // 跳过这段 bitpacked 的位段，回到下一个字节边界
                encode_pos = (bitStart + remainder * bw_i + 7) / 8;
            } else {
                // RLE: 先读 runCount (2 bytes)，随后是 run_length[]（bw 位）和 rle_values[]（bw_i 位）
                int runCount = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);
                encode_pos += 2;

                // 读取 run_length（每项 bw 位）
                int[] run_length = new int[runCount];
                encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, bw, runCount, run_length);

                // 读取 rle_values（每项 bw_i 位）
                int[] rle_values = new int[runCount];
                encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, bw_i, runCount, rle_values);

                // 根据 run_lengths 展开并赋值
                int pos = 0;
                for (int r = 0; r < runCount && pos < remainder; r++) {
                    int len = run_length[r];
                    int val = rle_values[r];
                    for (int t = 0; t < len && pos < remainder; t++, pos++) {
                        blockValues[pos] |= (val << (i * beta));
                    }
                }
                // 注意：encode_pos 已由 decodeBitPacking 更新
            }
        }

        // 7) 把 min_delta 加回每个位置
        for (int p = 0; p < remainder; p++) {
            blockValues[p] += min_delta;
        }

        return new DecodedBlock(blockValues, encode_pos);
    }

    /**
     * 完整解码整列（逐块调用 decodeBlockValues）
     *
     * @param encoded_result 编码后字节数组（包含前 8 字节 header: data_length, block_size）
     * @return 解码后的整列 int[]，长度 = data_length
     */
    public static long[] decodeColumnFully(byte[] encoded_result) {
        int encode_pos = 0;
        int data_length = readInt32BE(encoded_result, encode_pos);
        encode_pos += 4;
        int block_size = readInt32BE(encoded_result, encode_pos);
        encode_pos += 4;

        int num_blocks = data_length / block_size;
        int remainder = data_length % block_size;
        long[] out = new long[data_length];

        // 完整块
        for (int b = 0; b < num_blocks; b++) {
            DecodedBlock db = decodeBlockValues(encoded_result, b, block_size, block_size, encode_pos);
            encode_pos = db.newEncodePos;
            System.arraycopy(db.values, 0, out, b * block_size, block_size);
        }

        // 最后一块（如果有剩余）
        if (remainder > 0) {
            DecodedBlock db = decodeBlockValues(encoded_result, num_blocks, block_size, remainder, encode_pos);
            encode_pos = db.newEncodePos;
            System.arraycopy(db.values, 0, out, num_blocks * block_size, remainder);
        }

        return out;
    }

    /**
     * 严格 EM-parallel: 彻底解码两列为 int[]，然后逐行判断两个谓词同时成立的位置。
     *
     * @param encoded_result1 列1的编码字节数组
     * @param encoded_result2 列2的编码字节数组
     * @param upper_bound1    列1的上界谓词（< upper_bound1）
     * @param upper_bound2    列2的上界谓词（< upper_bound2）
     * @param result          用于输出匹配位置的数组（全表偏移 / 行号）
     * @param result_length   长度容器（长度为1的数组，写回匹配数）
     */
    public static void QueryTwoColumnsStrictEMParallel(byte[] encoded_result1, byte[] encoded_result2, int upper_bound1,
                                                       int upper_bound2, long[] result, int[] result_length) {

        // 完整解码两列
        long[] col1 = decodeColumnFully(encoded_result1);
        long[] col2 = decodeColumnFully(encoded_result2);

        // 确定行数（取两列最小）
        int n = Math.min(col1.length, col2.length);
        result_length[0] = 0;

        for (int i = 0; i < n; i++) {
            if (col1[i] < upper_bound1 && col2[i] < upper_bound2) {
                result[result_length[0]] = i;
                result_length[0]++;
            }
        }
    }

    /**
     * 严格 EM-pipelined: 完整解码第一列（物化为 values），根据第一列筛出 candidate positions（按块组织），
     * 然后对第二列按块按需解码（只解包含 candidate 的块），在这些块中对 candidate 的局部索引做精确判定。
     *
     * @param encoded_result1 列1编码
     * @param encoded_result2 列2编码
     * @param upper_bound1
     * @param upper_bound2
     * @param result
     * @param result_length
     */
    public static void QueryTwoColumnsStrictEMPipelined(byte[] encoded_result1, byte[] encoded_result2, int upper_bound1,
                                                        int upper_bound2, long[] result, int[] result_length) {

        // 1) 解码第 1 列（完整解码以物化 tuple 的该属性）
        long[] col1 = decodeColumnFully(encoded_result1);
        int data_length = col1.length;

        // 2) 构建候选位置分块索引（与 QueryWithIndices 中的策略一致）
        int block_size = readInt32BE(encoded_result2, 4); // 注意：编码头部：前4字节 data_length，接着 4 字节 block_size
        int num_blocks = data_length / block_size;
        int remainder = data_length % block_size;

        // 统计每个块中 candidate 数量
        int[] blockCount = new int[num_blocks + 1];
        for (int i = 0; i < data_length; i++) {
            if (col1[i] < upper_bound1) {
                int bidx = i / block_size;
                blockCount[bidx]++;
            }
        }

        // 若没有 candidate，快速返回
        int totalCandidates = 0;
        for (int c : blockCount) totalCandidates += c;
        result_length[0] = 0;
        if (totalCandidates == 0) return;

        // 为每块分配数组以记录 local indices
        int[][] blockIndices = new int[num_blocks + 1][];
        for (int b = 0; b <= num_blocks; b++) {
            blockIndices[b] = new int[blockCount[b]];
        }
        int[] cursor = new int[num_blocks + 1];
        for (int i = 0; i < data_length; i++) {
            if (col1[i] < upper_bound1) {
                int bidx = i / block_size;
                int local = i % block_size;
                blockIndices[bidx][cursor[bidx]++] = local;
            }
        }

        // 3) 逐块扫描第二列：若该块没有 candidate -> 跳过（SkipBlock）；否则解码该块并测试
        int encode_pos = 0;
        int data_len_from_header = readInt32BE(encoded_result2, encode_pos);
        encode_pos += 4;
        int bs_from_header = readInt32BE(encoded_result2, encode_pos);
        encode_pos += 4;
        // sanity check bs_from_header == block_size
        // 逐块循环
        for (int b = 0; b < num_blocks; b++) {
            if (blockCount[b] == 0) {
                // 跳过此块
                encode_pos = SkipBlock(encoded_result2, b, block_size, block_size, encode_pos);
                continue;
            }
            // 需要解码此块
            DecodedBlock db = decodeBlockValues(encoded_result2, b, block_size, block_size, encode_pos);
            encode_pos = db.newEncodePos;
            long[] vals = db.values;
            // 对该块的候选局部索引测试第二列谓词
            for (int j = 0; j < blockCount[b]; j++) {
                int localIdx = blockIndices[b][j];
                if (localIdx >= 0 && localIdx < vals.length) {
                    if (vals[localIdx] < upper_bound2) {
                        int globalPos = b * block_size + localIdx;
                        result[result_length[0]] = globalPos;
                        result_length[0]++;
                    }
                }
            }
        }

        // 最后可能的不完整块
        if (remainder > 0) {
            int b = num_blocks;
            if (blockCount[b] > 0) {
                DecodedBlock db = decodeBlockValues(encoded_result2, b, block_size, remainder, encode_pos);
                encode_pos = db.newEncodePos;
                long[] vals = db.values;
                for (int j = 0; j < blockCount[b]; j++) {
                    int localIdx = blockIndices[b][j];
                    if (localIdx >= 0 && localIdx < vals.length) {
                        if (vals[localIdx] < upper_bound2) {
                            int globalPos = b * block_size + localIdx;
                            result[result_length[0]] = globalPos;
                            result_length[0]++;
                        }
                    }
                }
            } else {
                // 无 candidate，跳过或不处理
            }
        }
    }

    public static double computeSelectivity(long len1_0, long len2_0, long halfSize, long match) {
        double sA = Double.NaN, sB = Double.NaN, pAB = Double.NaN, lift = Double.NaN;

        if (halfSize <= 0) return lift;

        // 基本概率
        sA = (double) len1_0 / (double) halfSize;
        sB = (double) len2_0 / (double) halfSize;
        pAB = (double) match / (double) halfSize;

        // lift = P(A∧B) / (P(A) P(B))，仅在分母非零时计算
        if (sA > 0.0 && sB > 0.0) {
            lift = pAB / (sA * sB);
        }

        return lift;
    }
    public static double phiCoefficient(long len1_0, long len2_0, long halfSize, long match) {
        // 2x2 表格元素
        double a = (double) match;                    // A ∧ B
        double b = (double) (len1_0 - match);         // A ∧ ¬B
        double c = (double) (len2_0 - match);         // ¬A ∧ B
        double d = (double) (halfSize - (match + (len1_0 - match) + (len2_0 - match)));
        // 等价于: d = halfSize - (a + b + c)

        // 如果任何分量为负，输入可能不合法，返回 NaN
        if (a < 0 || b < 0 || c < 0 || d < 0) {
            return Double.NaN;
        }

        double numerator = a * d - b * c;
        double denomTerm1 = (a + b) * (c + d);
        double denomTerm2 = (a + c) * (b + d);

        // 分母为 sqrt( denomTerm1 * denomTerm2 )
        double denomProduct = denomTerm1 * denomTerm2;
        if (denomProduct <= 0.0) {
            return Double.NaN; // 避免除零或根号负数
        }

        double phi = numerator / Math.sqrt(denomProduct);
        return phi;
    }
    // 放在类的末尾，作为一个新的测试 / helper
    @Test
    public void compareMaterializationStrategies() throws IOException {
        // --- 基本设置，复用你 testQuery 中的路径 / 数据准备逻辑 ---
        // String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/subcolumn/";
        String parent_dir = "D:/github/xjz17/subcolumn/";
        String input_parent_dir = parent_dir + "dataset/";
        // String output_parent_dir = parent_dir + "result/materialization/";

        String output_parent_dir = "D:/encoding-subcolumn/result/materialization/";

//        // 这里为了演示，仅处理单个 CSV 文件（你可以循环多个文件）
//        File directory = new File(input_parent_dir);
//        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));
//        if (csvFiles == null || csvFiles.length == 0) {
//            System.out.println("No csv files found under " + input_parent_dir);
//            return;
//        }
//
//        // 选第一个文件作为 demo
//        File file = csvFiles[0];
//        String datasetName = extractFileName(file.toString());
//        System.out.println("Dataset: " + datasetName);

        HashMap<String, Integer> queryRange = new HashMap<>();

        queryRange.put("Bird-migration", 2600000);
        queryRange.put("Bitcoin-price", 170000000);
        queryRange.put("City-temp", 700);
        queryRange.put("Dewpoint-temp", 9600);
        queryRange.put("IR-bio-temp", -200);
        queryRange.put("PM10-dust", 2000);
        queryRange.put("Stocks-DE", 90000);
        queryRange.put("Stocks-UK", 75000);
        queryRange.put("Stocks-USA", 6000);
        queryRange.put("Wind-Speed", 60);
        queryRange.put("Wine-Tasting", 10);
        queryRange.put("Arade4", 10000000);
        queryRange.put("EPM-Education", 200);
        queryRange.put("POI-lat", 0);
        queryRange.put("Gov10", 100000);

        int repeatTime = 500;
        String outputPath = output_parent_dir + "subcolumn_materialization.csv";
        CsvWriter writer = new CsvWriter(outputPath, ',', StandardCharsets.UTF_8);
        writer.setRecordDelimiter('\n');
        String[] head = {
                "Dataset",
                "Encoding Algorithm",
                "LM-pipelined",
                "LM-parallel",
                "EM-pipelined",
                "EM-parallel",
                "Points",
        };
        writer.writeRecord(head);
        File directory = new File(input_parent_dir);
        // File[] csvFiles = directory.listFiles();
        File[] csvFiles = directory.listFiles((dir, name) -> name.endsWith(".csv"));

        for (File file : csvFiles) {
            // 读取列并构造两列（复用你已有的读取逻辑）
            String datasetName = extractFileName(file.toString());
            System.out.println(datasetName);
            if(!queryRange.containsKey(datasetName))
                continue;

            InputStream inputStream = Files.newInputStream(file.toPath());
            CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
            ArrayList<Float> raw = new ArrayList<>();

            int max_decimal = 0;
            while (loader.readRecord()) {
                String f_str = loader.getValues()[0];
                if (f_str.isEmpty()) continue;
                int cur_decimal = getDecimalPrecision(f_str);
                if (cur_decimal > max_decimal) max_decimal = cur_decimal;
                raw.add(Float.valueOf(f_str));
            }
            inputStream.close();

            int totalSize = raw.size();
            int halfSize = totalSize / 2;
            long[] col1_data = new long[halfSize];
            long[] col2_data = new long[halfSize];
            int max_mul = (int) Math.pow(10, max_decimal);
            for (int i = 0; i < halfSize; i++) col1_data[i] = (long) (raw.get(i) * max_mul);
            for (int i = 0; i < halfSize; i++) col2_data[i] = (long) (raw.get(i + halfSize) * max_mul);

//            if(datasetName.equals("Stocks-UK")){
//                System.out.println(Arrays.toString(col1_data));
//            }
            int block_size = 512; // 选择一个 block size 做比较
//            int repeatTime = 200;
            byte[] encoded_result1 = new byte[col1_data.length * 8];
            byte[] encoded_result2 = new byte[col2_data.length * 8];

            // 编码（复用你的 Encoder）
            int length1 = SubcolumnLongTest.Encoder(col1_data, block_size, encoded_result1);
            int length2 = SubcolumnLongTest.Encoder(col2_data, block_size, encoded_result2);

            int upper = queryRange.containsKey(datasetName) ? queryRange.get(datasetName) : Integer.MAX_VALUE;

//            System.out.println("Running repeats: " + repeatTime + " upper=" + upper);

            // ---------- 1) LM-pipelined: your existing QueryTwoColumns ----------
            long tStart = System.nanoTime();
            for (int r = 0; r < repeatTime; r++) {
                QueryTwoColumns(encoded_result1, encoded_result2, upper, upper);
            }
            long tEnd = System.nanoTime();
            long lmPipelinedTime = (tEnd - tStart) / repeatTime;
            System.out.println("LM-pipelined avg ns: " + lmPipelinedTime);

            // ---------- 2) LM-parallel: Query both columns separately -> intersect positions ----------
            // helper arrays reused
            int[] res1 = new int[encoded_result1.length];
            int[] len1 = new int[1];
            int[] res2 = new int[encoded_result2.length];
            int[] len2 = new int[1];

            // warm run to avoid JIT one-time overhead bias
            Query(encoded_result1, upper, res1, len1);
            Query(encoded_result2, upper, res2, len2);

            double selectivity = 0;
            double phi = 0;
            int match = 0;
            tStart = System.nanoTime();
            for (int r = 0; r < repeatTime; r++) {
                // run both queries (they are pure functions on encoded bytes)
                CompletableFuture<Void> future1 = CompletableFuture.runAsync(() -> {
                    Query(encoded_result1, upper, res1, len1);
                });

                CompletableFuture<Void> future2 = CompletableFuture.runAsync(() -> {
                    Query(encoded_result2, upper, res2, len2);
                });

                // 等待两个查询都完成
                try {
                    CompletableFuture.allOf(future1, future2).get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    // 处理异常，可能需要中断循环或采取其他措施
                    Thread.currentThread().interrupt(); // 重新设置中断状态
                    break;
                }
                // intersect result sets (they are arrays of positions)
//System.out.println(len1[0]);
//                System.out.println(len2[0]);
//// 并行设置bit
                long[] bits1 = new long[(halfSize + 63) / 64];
                long[] bits2 = new long[(halfSize + 63) / 64];

// 设置bit
                for (int i = 0; i < len1[0]; i++) {
                    int pos = res1[i];
                    bits1[pos >> 6] |= (1L << (pos & 0x3F));
                }

                for (int i = 0; i < len2[0]; i++) {
                    int pos = res2[i];
                    bits2[pos >> 6] |= (1L << (pos & 0x3F));
                }

// 求交集并计数
                match = 0;
                for (int i = 0; i < bits1.length; i++) {
                    long intersection = bits1[i] & bits2[i];
                    match += Long.bitCount(intersection);
                }
            }
            tEnd = System.nanoTime();
//            selectivity = computeSelectivity(len1[0],len2[0],halfSize,match);
//            phi  = phiCoefficient(len1[0],len2[0],halfSize,match);
//            System.out.println(len1[0]+","+len2[0]);
            long lmParallelTime = (tEnd - tStart) / repeatTime;
            System.out.println("LM-parallel avg ns: " + lmParallelTime);


            long[] result = new long[encoded_result1.length];
            int[] resultLen = new int[1];

// ---------- 3) EM-pipelined (近似实现说明) ----------
            // 说明：严格的 EM-pipelined 需要把第一列物化成 (pos,value) tuples（即完整解码得到值），
            // 然后按这些 pos 去第二列延展并筛选。要做到严格，需实现「按索引只解码第二列对应位置的值」或把第二列解码成数组。
            // 在这里给出一个“可运行的近似实现”：把第一列先用 Query() 拿到候选 positions（pos），
            // 再把这些 pos 作为 candidate 传入 QueryWithIndices(encoded_result2,...)
            // （注意：这是 LM-pipelined 与 EM-pipelined 在含义上并非完全相同，但在当前可用接口下这是能运行且可比较的实现）
            tStart = System.nanoTime();
            for (int r = 0; r < repeatTime; r++) {
                // 获取第一列候选位置（认为已“物化”成pos list）
                resultLen[0] = 0;
                QueryTwoColumnsStrictEMPipelined(encoded_result1, encoded_result2, queryRange.get(datasetName), queryRange.get(datasetName), result, resultLen);

//                Query(encoded_result1, upper, res1, len1);
//                // 用这些位置去查询第二列（QueryWithIndices 将仅在这些位置上做判断）
//                QueryWithIndices(encoded_result2, upper, res1, len1[0], res2, len2);
//                // res2 中现在是满足第二列 < upper 的偏移（相对于全列偏移），如果需要对第一列的值也做判定，需要把第一列解码成values，这里省略
            }
            tEnd = System.nanoTime();
            long emPipelinedApproxTime = (tEnd - tStart) / repeatTime;
//            System.out.println("EM-pipelined strict matched: " + resultLen[0]);
            System.out.println("EM-pipelined (approx) avg ns: " + emPipelinedApproxTime);

            // ---------- 4) EM-parallel ----------

            tStart = System.nanoTime();
            for (int r = 0; r < repeatTime; r++) {
                // 近似实现：同时对两列调用 QueryWithIndices（先对第一列获取候选pos，然后把这些pos作为输入去第二列）
                QueryTwoColumnsStrictEMParallel(encoded_result1, encoded_result2, queryRange.get(datasetName), queryRange.get(datasetName), result, resultLen);
//                Query(encoded_result1, upper, res1, len1); // first column candidates
//                QueryWithIndices(encoded_result2, upper, res1, len1[0], res2, len2);
                // 结果 res2 表示在那些 pos 中满足第二列条件的偏移
            }
            tEnd = System.nanoTime();
//            System.out.println("EM-parallel strict matched: " + resultLen[0]);
            long emParallelApproxTime = (tEnd - tStart) / repeatTime;
            System.out.println("EM-parallel (approx) avg ns: " + emParallelApproxTime);

            // 最后打印一行小结
            System.out.println("Summary (ns avg per query): LM-pipelined=" + lmPipelinedTime
                    + " LM-parallel=" + lmParallelTime
                    + " EM-pipelined-approx=" + emPipelinedApproxTime
                    + " EM-parallel-approx=" + emParallelApproxTime);

            String[] record = {
                    datasetName,
                    "Sub-columns",
                    String.valueOf(lmPipelinedTime),
                    String.valueOf(lmParallelTime),
                    String.valueOf(emPipelinedApproxTime),
                    String.valueOf(emParallelApproxTime),
                    String.valueOf(totalSize)
            };


            writer.writeRecord(record);

        }
        writer.close();
    }

}
