package org.apache.iotdb.tsfile.encoding;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

public class SubcolumnQueryMaxNewTest {

    public static void Query(byte[] encoded_result) {
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
            encodePos = BlockQueryMax(encoded_result, i, block_size, block_size, encodePos, result, result_length);
        }

        // 处理剩余部分
        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = ((encoded_result[encodePos] & 0xFF) << 24)
                        | ((encoded_result[encodePos + 1] & 0xFF) << 16)
                        | ((encoded_result[encodePos + 2] & 0xFF) << 8)
                        | (encoded_result[encodePos + 3] & 0xFF);
                result[result_length[0]] = value;
                result_length[0]++;
                encodePos += 4;
            }
        } else {
            encodePos = BlockQueryMax(encoded_result, num_blocks, block_size, remainder, encodePos, result, result_length);
        }
    }

    public static int BlockQueryMax(byte[] encoded_result, int block_index, int block_size, int remainder,
                                     int encodePos, int[] result, int[] result_length) {
        // 读取最小增量
        int minDelta0 = ((encoded_result[encodePos] & 0xFF) << 24)
                | ((encoded_result[encodePos + 1] & 0xFF) << 16)
                | ((encoded_result[encodePos + 2] & 0xFF) << 8)
                | (encoded_result[encodePos + 3] & 0xFF);
        encodePos += 4;

        int m = encoded_result[encodePos] & 0xFF;
        encodePos += 1;

        // 初始化候选索引
        int[] candidate_indices = new int[remainder];
        for (int i = 0; i < remainder; i++) {
            candidate_indices[i] = i;
        }

        if (m == 0) {
            result[result_length[0]] = minDelta0;
            result_length[0]++;
            return encodePos;
        }

        // 获取比特宽度和编码类型
        int bw = SubcolumnTest.bitWidth(block_size);
        int beta = encoded_result[encodePos] & 0xFF;
        encodePos += 1;

        int l = (m + beta - 1) / beta;
        int[] bitWidthList = new int[l];
        encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, 8, l, bitWidthList);

        int[] encodingType = new int[l];
        encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, 1, l, encodingType);

        // 处理每个子列
        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) {
                if (candidate_indices.length == 1) {
                    // Skip if there's only one candidate
                    long bitOffset = encodePos * 8L + (long) bitWidthList[i] * remainder;
                    encodePos = (int) ((bitOffset + 7) / 8);
                    continue;
                }

                // 处理最大值
                int maxPart = Integer.MIN_VALUE; // 初始化为最小值
                int new_length = 0;

                for (int j = 0; j < candidate_indices.length; j++) {
                    int index = candidate_indices[j];
                    int value = SubcolumnTest.bytesToInt(encoded_result, encodePos * 8 + index * bitWidthList[i], bitWidthList[i]);

                    if (value > maxPart) {
                        maxPart = value;
                        new_length = 0;
                        candidate_indices[new_length++] = index;
                    } else if (value == maxPart) {
                        candidate_indices[new_length++] = index;
                    }
                }

                // 更新位移
                encodePos = (int) (((long) encodePos * 8 + bitWidthList[i] * remainder + 7) / 8);
                candidate_indices = resizeArray(candidate_indices, new_length); // Resize to keep only valid indices
            } else {
                int index = ((encoded_result[encodePos] & 0xFF) << 8) | (encoded_result[encodePos + 1] & 0xFF);
                encodePos += 2;

                if (candidate_indices.length == 1) {
                    long bitOffset = encodePos * 8L + bw * index;
                    encodePos = (int) ((bitOffset + 7) / 8);
                    bitOffset = encodePos * 8L + (long) bitWidthList[i] * index;
                    encodePos = (int) ((bitOffset + 7) / 8);
                    continue;
                }

                // 处理RLE部分
                int[] run_length = new int[index];
                int[] rle_values = new int[index];
                encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, bw, index, run_length);
                encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, bitWidthList[i], index, rle_values);

                int maxPart = Integer.MIN_VALUE;
                int new_length = 0;
                int rleIndex = 0;
                int currentPos = 0;

                for (int j = 0; j < candidate_indices.length; j++) {
                    int index_candidate = candidate_indices[j];
                    while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index && rle_values[rleIndex] > maxPart) {
                        maxPart = rle_values[rleIndex];
                        new_length = 1;
                        candidate_indices[0] = index_candidate;  // 初始化为第一个
                    } else if (rleIndex < index && rle_values[rleIndex] == maxPart) {
                        candidate_indices[new_length++] = index_candidate;  // 添加当前候选
                    }
                }
                candidate_indices = resizeArray(candidate_indices, new_length); // Resize to keep only valid indices
            }
        }

        // 记录最大值到结果中
        result[result_length[0]] = candidate_indices[0];
        result_length[0]++;

        return encodePos;
    }

    private static int[] resizeArray(int[] array, int newSize) {
        int[] newArray = new int[newSize];
        System.arraycopy(array, 0, newArray, 0, Math.min(array.length, newSize));
        return newArray;
    }
}
