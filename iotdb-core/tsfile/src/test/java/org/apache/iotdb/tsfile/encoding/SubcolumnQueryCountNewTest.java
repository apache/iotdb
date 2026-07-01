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

public class SubcolumnQueryCountNewTest {

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
        int[] result = new int[1]; // 只记录计数
        result[0] = 0;

        // 处理每个块
        for (int i = 0; i < num_blocks; i++) {
            encodePos = BlockQueryCount(encoded_result, i, block_size,
                    block_size, encodePos, target, result);
        }

        // 处理剩余部分
        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = SubcolumnTest.bytes2Integer(encoded_result, encodePos, 4);
                encodePos += 4;
                if (value == target) {
                    result[0]++;
                }
            }
        } else {
            encodePos = BlockQueryCount(encoded_result, num_blocks, block_size,
                    remainder, encodePos, target, result);
        }
    }

    public static int BlockQueryCount(byte[] encoded_result, int block_index, int block_size, int remainder,
                                       int encodePos, int target, int[] result) {

        // 读取最小增量
        int minDelta0 = SubcolumnTest.bytes2Integer(encoded_result, encodePos, 4);
        encodePos += 4;

        int m = encoded_result[encodePos] & 0xFF;
        encodePos += 1;

        target -= minDelta0; // 调整目标值
        
        // 初始化候选索引
        int[] candidate_indices = new int[remainder];
        int candidate_length = remainder;
        for (int i = 0; i < remainder; i++) {
            candidate_indices[i] = i;
        }

        if (m == 0) {
            if (target == 0) {
                result[0] += remainder;
            }
            return encodePos;
        }

        int bitWidth = SubcolumnTest.bitWidth(block_size);
        int beta = encoded_result[encodePos] & 0xFF;
        encodePos += 1;

        // 计算子列数
        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];
        encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, 8, l, bitWidthList);

        int[] encodingType = new int[l];
        encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, 1, l, encodingType);

        // 处理每个子列
        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) { // 类型 0：plain bit-packed
                if (target < 0) {
                    long bitPos = ((long) encodePos) * 8L + (long) bitWidthList[i] * (long) remainder;
                    encodePos = (int) ((bitPos + 7) / 8);
                    continue;
                }

                long bitPos = ((long) encodePos) * 8L;

                int new_length = 0;
                for (int j = 0; j < candidate_length; j++) {
                    int index = candidate_indices[j];
                    int current = SubcolumnTest.bytesToInt(encoded_result,
                            (int)(bitPos + index * bitWidthList[i]), bitWidthList[i]);
                    int value = (target >> (i * beta)) & ((1 << beta) - 1);
                    if (current == value) {
                        candidate_indices[new_length++] = index;
                    }
                }

                candidate_length = new_length;
                bitPos += (long) remainder * bitWidthList[i];
                encodePos = (int) ((bitPos + 7) / 8);

            } else { // 类型 1：RLE + bitpacked values
                int index = ((encoded_result[encodePos] & 0xFF) << 8) | (encoded_result[encodePos + 1] & 0xFF);
                encodePos += 2;

                if (target < 0) {
                    long bitPos = ((long) encodePos) * 8L + (long) bitWidth * index;
                    encodePos = (int) ((bitPos + 7) / 8);
                    bitPos = ((long) encodePos) * 8L + (long) bitWidthList[i] * index;
                    encodePos = (int) ((bitPos + 7) / 8);
                    continue;
                }

                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, bitWidth, index, run_length);
                encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, bitWidthList[i], index, rle_values);

                int new_length = 0;
                int rleIndex = 0;
                int currentPos = 0;
                int value = (target >> (i * beta)) & ((1 << beta) - 1);

                for (int j = 0; j < candidate_length; j++) {
                    int index_candidate = candidate_indices[j];

                    while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index && rle_values[rleIndex] == value) {
                        candidate_indices[new_length++] = index_candidate;
                    }
                }

                candidate_length = new_length;
            }
        }

        result[0] += candidate_length; // 统计符合条件的总数

        return encodePos;
    }

}
