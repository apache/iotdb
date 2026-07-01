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

public class SubcolumnQueryGreaterLessTest {

    public static void Query(byte[] encoded_result, int lower_bound, int upper_bound) {

        int encode_pos = 0;

        int data_length = SubcolumnTest.bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int block_size = SubcolumnTest.bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        // 查询结果
        int[] result = new int[data_length];
        int[] result_length = new int[1];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockQueryIndex(encoded_result, i, block_size,
                    block_size, encode_pos, lower_bound, upper_bound,
                    result, result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = SubcolumnTest.bytes2Integer(encoded_result, encode_pos, 4);
                if (value > lower_bound && value < upper_bound) {
                    result[result_length[0]] = value;
                    result_length[0]++;
                }
                encode_pos += 4;
            }
        } else {
            encode_pos = BlockQueryIndex(encoded_result, num_blocks, block_size,
                    remainder, encode_pos, lower_bound, upper_bound,
                    result, result_length);
        }

    }

    public static int BlockQueryIndex(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int lower_bound, int upper_bound, int[] result, int[] result_length) {
        int[] min_delta = new int[3];

        min_delta[0] = SubcolumnTest.bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        // int[] block_data = new int[remainder];

        // int m = encoded_result[encode_pos];
        int m = SubcolumnTest.bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        lower_bound -= min_delta[0];
        upper_bound -= min_delta[0];

        // 候选索引
        int[] candidate_indices = new int[remainder];
        int candidate_length = 0;
        for (int i = 0; i < remainder; i++) {
            candidate_indices[i] = i;
            candidate_length++;
        }

        if (m == 0) {
            if (lower_bound < 0 && upper_bound > 0) {
                for (int i = 0; i < remainder; i++) {
                    result[result_length[0]] = block_size * block_index + i;
                    result_length[0]++;
                }
            }
            return encode_pos;
        }

        int bw = SubcolumnTest.bitWidth(block_size);

        // int beta = encoded_result[encode_pos];
        int beta = SubcolumnTest.bytes2Integer(encoded_result, encode_pos, 1);
        encode_pos += 1;

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        int[][] subcolumnList = new int[l][remainder];

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
                    int lower_value = (lower_bound >> (i * beta)) & ((1 << beta) - 1);
                    int upper_value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);
                    if (subcolumnList[i][index] > lower_value && subcolumnList[i][index] < upper_value) {
                        result[result_length[0]] = block_size * block_index + index;
                        result_length[0]++;
                    } else if (subcolumnList[i][index] == lower_value 
                            || subcolumnList[i][index] == upper_value) {
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
                int lower_value = (lower_bound >> (i * beta)) & ((1 << beta) - 1);
                int upper_value = (upper_bound >> (i * beta)) & ((1 << beta) - 1);

                for (int j = 0; j < candidate_length; j++) {
                    int index_candidate = candidate_indices[j];

                    while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index) {
                        if (rle_values[rleIndex] > lower_value && rle_values[rleIndex] < upper_value) {
                            result[result_length[0]] = block_size * block_index + index_candidate;
                            result_length[0]++;
                        } else if (rle_values[rleIndex] == lower_value 
                                || rle_values[rleIndex] == upper_value) {
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

}
