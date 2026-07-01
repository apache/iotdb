package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class SubcolumnQueryEqualNewTest {

    public static void Query(byte[] encoded_result, int target) {
        int encodePos = 0;

        // 解析数据长度
        int data_length = ((encoded_result[encodePos] & 0xFF) << 24)
                | ((encoded_result[encodePos + 1] & 0xFF) << 16)
                | ((encoded_result[encodePos + 2] & 0xFF) << 8)
                | (encoded_result[encodePos + 3] & 0xFF);
        encodePos += 4;

        // 解析块大小
        int block_size = ((encoded_result[encodePos] & 0xFF) << 24)
                | ((encoded_result[encodePos + 1] & 0xFF) << 16)
                | ((encoded_result[encodePos + 2] & 0xFF) << 8)
                | (encoded_result[encodePos + 3] & 0xFF);
        encodePos += 4;

        int num_blocks = data_length / block_size;

        // 查询结果
        int[] result = new int[data_length];
        int[] result_length = new int[1];

        // 处理每个块
        for (int i = 0; i < num_blocks; i++) {
            encodePos = BlockQueryIndex(encoded_result, i, block_size,
                    block_size, encodePos, target,
                    result, result_length);
        }

        // 处理剩余部分
        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = SubcolumnTest.bytes2Integer(encoded_result, encodePos, 4);
                encodePos += 4;
                if (value == target) {
                    result[result_length[0]] = value;
                    result_length[0]++;
                }
            }
        } else {
            encodePos = BlockQueryIndex(encoded_result, num_blocks, block_size,
                    remainder, encodePos, target, result, result_length);
        }
    }



    public static int BlockQueryIndex(byte[] encoded_result, int block_index, int block_size, int remainder,
                                      int encodePos, int target, int[] result, int[] result_length) {

        // 读取最小增量
        int minDelta0 = SubcolumnTest.bytes2Integer(encoded_result, encodePos, 4);
        encodePos += 4;

        int m = encoded_result[encodePos] & 0xFF;
        encodePos += 1;

        target -= minDelta0; // 调整目标值

        if (m == 0) {
            if (target == 0) {
                for (int i = 0; i < remainder; i++) {
                    result[result_length[0]++] = block_size * block_index + i;
                }
            }
            return encodePos;
        }

        // 初始化候选索引（一次分配）
        int[] candidate_indices = new int[remainder];
        int candidate_length = remainder;
        for (int i = 0; i < remainder; i++) candidate_indices[i] = i;

        int bw = SubcolumnTest.bitWidth(block_size);
        int beta = encoded_result[encodePos] & 0xFF;
        encodePos += 1;

        int l = (m + beta - 1) / beta;
        int[] bitWidthList = new int[l];
        encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, 8, l, bitWidthList);

        int[] encodingType = new int[l];
        encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, 1, l, encodingType);

        // 处理每个子列（从高位到低位）
        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            int bitWidth = bitWidthList[i];

            if (type == 0) { // plain bit-packed
                if (target < 0) {
                    // skip this subcolumn: compute byte position advance
                    long skipBits = (long) bitWidth * (long) remainder;
                    long bitPos = ((long) encodePos) * 8L + skipBits;
                    encodePos = (int) ((bitPos + 7L) >>> 3);
                    continue;
                }

                final int expectedValue = (target >> (i * beta)) & ((1 << beta) - 1);
                long baseBitPos = ((long) encodePos) * 8L; // start bit pos of this subcolumn

                // Heuristic: if many candidates remain, sequential scan is better;
                // otherwise random-access per candidate is better.
                if (candidate_length > (remainder >> 1)) {
                    // sequential scan across all remainder values
                    int new_length = 0;
                    long bitPos = baseBitPos;
                    for (int pos = 0; pos < remainder; pos++) {
                        int subValue = SubcolumnTest.bytesToInt(encoded_result, (int) (bitPos + (long) pos * bitWidth),
                                bitWidth);
                        if (subValue == expectedValue) {
                            candidate_indices[new_length++] = pos;
                        }
                    }
                    candidate_length = new_length;
                } else {
                    // random-access for just the candidate positions (current approach),
                    // but avoid recomputing expectedValue and some arithmetic.
                    int new_length = 0;
                    for (int j = 0; j < candidate_length; j++) {
                        int idx = candidate_indices[j];
                        int subValue = SubcolumnTest.bytesToInt(encoded_result,
                                (int) (baseBitPos + (long) idx * bitWidth), bitWidth);
                        if (subValue == expectedValue) {
                            candidate_indices[new_length++] = idx;
                        }
                    }
                    candidate_length = new_length;
                }

                // advance encodePos past this packed block
                long advBits = (long) bitWidth * (long) remainder;
                long endBitPos = baseBitPos + advBits;
                encodePos = (int) ((endBitPos + 7L) >>> 3);

            } else { // type == 1: RLE + bitpacked values
                // read length of runs (index)
                int index = ((encoded_result[encodePos] & 0xFF) << 8) | (encoded_result[encodePos + 1] & 0xFF);
                encodePos += 2;

                if (target < 0) {
                    // skip both run_length and rle_values payloads:
                    long skipBitsRunLens = (long) bw * index;
                    long bitPos = ((long) encodePos) * 8L + skipBitsRunLens;
                    encodePos = (int) ((bitPos + 7L) >>> 3);

                    long skipBitsValues = (long) bitWidth * index;
                    bitPos = ((long) encodePos) * 8L + skipBitsValues;
                    encodePos = (int) ((bitPos + 7L) >>> 3);
                    continue;
                }

                // decode run lengths and values
                int[] run_length = new int[index];
                int[] rle_values = new int[index];
                encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, bw, index, run_length);
                encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, bitWidth, index, rle_values);

                final int expectedValue = (target >> (i * beta)) & ((1 << beta) - 1);

                // Candidate indices are sorted; advance rleIndex monotonically.
                int new_length = 0;
                int rleIndex = 0;
                int currentPos = 0; // current starting pos of the run rleIndex

                for (int j = 0; j < candidate_length; j++) {
                    int idxCandidate = candidate_indices[j];

                    // move rleIndex forward until its run covers idxCandidate (or we run out)
                    while (rleIndex < index && currentPos + run_length[rleIndex] <= idxCandidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index && rle_values[rleIndex] == expectedValue) {
                        // idxCandidate falls into a run with matching value
                        candidate_indices[new_length++] = idxCandidate;
                    }
                }

                candidate_length = new_length;
            }
        }

        // 输出最终匹配的索引
        for (int i = 0; i < candidate_length; i++) {
            result[result_length[0]++] = block_size * block_index + candidate_indices[i];
        }

        return encodePos;
    }

}
