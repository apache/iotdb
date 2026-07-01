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

public class SubcolumnQueryGreaterNewTest {
    public static void Query(byte[] encoded_result, int lower_bound) {
        int encodePos = 0;

        int data_length = ((encoded_result[encodePos] & 0xFF) << 24)
                | ((encoded_result[encodePos + 1] & 0xFF) << 16)
                | ((encoded_result[encodePos + 2] & 0xFF) << 8)
                | (encoded_result[encodePos + 3] & 0xFF);
        encodePos += 4;

        int block_size = ((encoded_result[encodePos] & 0xFF) << 24)
                | ((encoded_result[encodePos + 1] & 0xFF) << 16)
                | ((encoded_result[encodePos + 2] & 0xFF) << 8)
                | (encoded_result[encodePos + 3] & 0xFF);
        encodePos += 4;

        int num_blocks = data_length / block_size;

        int[] result = new int[data_length];
        int[] result_length = new int[1];

        for (int i = 0; i < num_blocks; i++) {
            encodePos = BlockQueryIndex(encoded_result, i, block_size, block_size,
                    encodePos, lower_bound, result, result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = ((encoded_result[encodePos] & 0xFF) << 24)
                        | ((encoded_result[encodePos + 1] & 0xFF) << 16)
                        | ((encoded_result[encodePos + 2] & 0xFF) << 8)
                        | (encoded_result[encodePos + 3] & 0xFF);
                if (value > lower_bound) {
                    result[result_length[0]] = value;
                    result_length[0]++;
                }
                encodePos += 4;
            }
        } else {
            encodePos = BlockQueryIndex(encoded_result, num_blocks, block_size,
                    remainder, encodePos, lower_bound, result, result_length);
        }

        // 可选：返回结果或者把 result/result_length 放到可访问位置
    }

    /**
     * 返回更新后的字节偏移 encodePos（保持和原来接口一致）。
     *
     * 注意：
     * - encodePos 传入/返回的是字节偏移（byte offset）。
     * - 当需要按位跳过数据时，使用临时 long bitPos = encodePos * 8L 来处理，再换算回字节偏移。
     */
    public static int BlockQueryIndex(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encodePos, int lower_bound, int[] result, int[] result_length) {

        // 只用第一个 min_delta（原代码也只用了第一个）
        int minDelta0 = ((encoded_result[encodePos] & 0xFF) << 24)
                | ((encoded_result[encodePos + 1] & 0xFF) << 16)
                | ((encoded_result[encodePos + 2] & 0xFF) << 8)
                | (encoded_result[encodePos + 3] & 0xFF);
        encodePos += 4;

        int m = encoded_result[encodePos] & 0xFF;
        encodePos += 1;

        int adjustedLower = lower_bound - minDelta0; // 不更改传入 lower_bound 的原始值

        // 初始化候选索引（全部候选）
        int[] candidate_indices = new int[Math.max(1, remainder)];
        int candidate_length = remainder;
        for (int i = 0; i < remainder; i++) {
            candidate_indices[i] = i;
        }

        if (m == 0) {
            if (adjustedLower < 0) {
                int baseIndex = block_size * block_index;
                for (int i = 0; i < remainder; i++) {
                    result[result_length[0]] = baseIndex + i;
                    result_length[0]++;
                }
            }
            return encodePos;
        }

        int bw = SubcolumnTest.bitWidth(block_size);
        int beta = encoded_result[encodePos] & 0xFF;
        encodePos += 1;

        int l = (m + beta - 1) / beta;
        int[] bitWidthList = new int[l];
        encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, 8, l, bitWidthList);

        int[] encodingType = new int[l];
        encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, 1, l, encodingType);

        int baseIndex = block_size * block_index;

        // 处理每个子列（从高到低，与原代码一致）
        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];

            if (type == 0) {
                // 类型 0：plain bit-packed
                if (adjustedLower <= 0) {
                    // 只跳过该子列的所有位宽 -> 使用位偏移处理避免破坏 encodePos 的语义
                    long bitPos = ((long) encodePos) * 8L + (long) bitWidthList[i] * (long) remainder;
                    encodePos = (int) ((bitPos + 7L) / 8L);
                    continue;
                }

                // 需要解码并检查候选
                long bitPos = ((long) encodePos) * 8L;
                int new_length = 0;

                // 每个候选索引读取该子列对应的 bit-width 值并比较
                int shiftMaskValue = (adjustedLower >> (i * beta)) & ((1 << beta) - 1);
                int bw_i = bitWidthList[i];

                for (int j = 0; j < candidate_length; j++) {
                    int idx = candidate_indices[j];
                    // 从 bitPos + idx*bw_i 处解出值（假设 bytesToInt 的第二个参数表示 bit-offset）
                    int bitOffsetForThis = (int) (bitPos + (long) idx * bw_i);
                    int subValue = SubcolumnTest.bytesToInt(encoded_result, bitOffsetForThis, bw_i);
                    if (subValue > shiftMaskValue) {
                        result[result_length[0]] = baseIndex + idx;
                        result_length[0]++;
                    } else if (subValue == shiftMaskValue) {
                        candidate_indices[new_length++] = idx;
                    }
                }

                candidate_length = new_length;
                // advance encodePos by remainder * bw_i bits
                bitPos += (long) remainder * bw_i;
                encodePos = (int) ((bitPos + 7L) / 8L);

            } else {
                // type == 1：RLE + bitpacked values
                // 先读 index（RLE segment count）
                int index = ((encoded_result[encodePos] & 0xFF) << 8) | (encoded_result[encodePos + 1] & 0xFF);
                encodePos += 2;

                if (adjustedLower <= 0) {
                    // 跳过 RLE 的两个区域（先按 bw 跳过 run_length，再按 bitWidthList 跳过 rle_values）
                    long bitPos = ((long) encodePos) * 8L;
                    bitPos += (long) bw * index;
                    encodePos = (int) ((bitPos + 7L) / 8L);

                    bitPos = ((long) encodePos) * 8L;
                    bitPos += (long) bitWidthList[i] * index;
                    encodePos = (int) ((bitPos + 7L) / 8L);
                    continue;
                }

                // 读取 run lengths（bw 位宽）和对应的 rle_values（bitWidthList[i] 位宽）
                int[] run_length = new int[index];
                int[] rle_values = new int[index];

                encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, bw, index, run_length);
                encodePos = SubcolumnTest.decodeBitPacking(encoded_result, encodePos, bitWidthList[i], index,
                        rle_values);

                // 遍历候选索引并用 RLE 查找对应的 value
                int new_length = 0;
                int rleIndex = 0;
                int currentPos = 0;
                int targetValue = (adjustedLower >> (i * beta)) & ((1 << beta) - 1);

                for (int j = 0; j < candidate_length; j++) {
                    int idx = candidate_indices[j];
                    // 移动到包含 idx 的 rle 段
                    while (rleIndex < index && currentPos + run_length[rleIndex] <= idx) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }
                    if (rleIndex < index) {
                        int rv = rle_values[rleIndex];
                        if (rv > targetValue) {
                            result[result_length[0]] = baseIndex + idx;
                            result_length[0]++;
                        } else if (rv == targetValue) {
                            candidate_indices[new_length++] = idx;
                        }
                    }
                }
                candidate_length = new_length;
            }
        }

        // 遍历所有子列后，如果 adjustedLower <= 0，则整块全部命中（和原逻辑一致）
        if (adjustedLower <= 0) {
            for (int i = 0; i < remainder; i++) {
                result[result_length[0]] = baseIndex + i;
                result_length[0]++;
            }
        }

        return encodePos;
    }

}
