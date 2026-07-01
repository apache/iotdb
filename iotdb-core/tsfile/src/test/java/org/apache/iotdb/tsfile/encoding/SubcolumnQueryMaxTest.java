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

public class SubcolumnQueryMaxTest {

    public static void Query(byte[] encoded_result) {

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
        int[] result = new int[data_length];
        int[] result_length = new int[1];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockQueryMax(encoded_result, i, block_size, block_size, encode_pos, result,
                    result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                int value = ((encoded_result[encode_pos] & 0xFF) << 24) |
                        ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                        ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
                result[result_length[0]] = value;
                result_length[0]++;
                encode_pos += 4;
            }
        } else {
            encode_pos = BlockQueryMax(encoded_result, num_blocks, block_size, remainder, encode_pos,
                    result, result_length);
        }

        // for (int i = 0; i < result_length[0]; i++) {
        // System.out.print(result[i] + " ");
        // }
        // System.out.println();

    }

    public static int BlockQueryMax(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int[] result, int[] result_length) {
        int[] min_delta = new int[3];

        min_delta[0] = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        int m = encoded_result[encode_pos];
        encode_pos += 1;

        // System.out.println("m: " + m);

        int[] candidate_indices = new int[remainder];
        int candidate_length = 0;
        for (int i = 0; i < remainder; i++) {
            candidate_indices[i] = i;
            candidate_length++;
        }

        if (m == 0) {
            result[result_length[0]] = min_delta[0];
            result_length[0]++;
            return encode_pos;
        }

        int bw = SubcolumnTest.bitWidth(block_size);

        int beta = encoded_result[encode_pos];
        encode_pos += 1;

        // System.out.println("beta: " + beta);

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        // int[][] subcolumnList = new int[l][remainder];

        int[] encodingType = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) {
                if (candidate_length == 1) {
                    encode_pos *= 8;
                    encode_pos += bitWidthList[i] * remainder;
                    encode_pos = (encode_pos + 7) / 8;
                    continue;
                }

                int maxPart = 0;

                int new_length = 0;
                for (int j = 0; j < candidate_length; j++) {
                    int index = candidate_indices[j];
                    int value = SubcolumnTest.bytesToInt(encoded_result,
                            encode_pos * 8 + index * bitWidthList[i], bitWidthList[i]);

                    if (value > maxPart) {
                        maxPart = value;

                        new_length = 0;
                        candidate_indices[new_length] = index;
                        new_length++;
                    } else if (value == maxPart) {
                        candidate_indices[new_length] = index;
                        new_length++;
                    }
                }

                encode_pos *= 8;
                encode_pos += bitWidthList[i] * remainder;
                encode_pos = (encode_pos + 7) / 8;

                // for (int j = 0; j < candidate_length; j++) {
                // int index = candidate_indices[j];
                // if (subcolumnList[i][index] == maxPart) {
                // candidate_indices[new_length] = index;
                // new_length++;
                // }
                // }

                candidate_length = new_length;

            } else {
                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);
                encode_pos += 2;

                if (candidate_length == 1) {
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

                int maxPart = 0;

                int new_length = 0;
                int rleIndex = 0;
                int currentPos = 0;

                for (int j = 0; j < candidate_length; j++) {
                    int index_candidate = candidate_indices[j];

                    while (rleIndex < index && currentPos + run_length[rleIndex] <= index_candidate) {
                        currentPos += run_length[rleIndex];
                        rleIndex++;
                    }

                    if (rleIndex < index) {
                        if (rle_values[rleIndex] > maxPart) {
                            maxPart = rle_values[rleIndex];

                            new_length = 0;
                        } else if (rle_values[rleIndex] == maxPart) {
                            candidate_indices[new_length] = index_candidate;
                            new_length++;
                        }
                    }
                }

                // for (int j = 0; j < candidate_length; j++) {
                // int index_candidate = candidate_indices[j];

                // while (rleIndex < index && currentPos + run_length[rleIndex] <=
                // index_candidate) {
                // currentPos += run_length[rleIndex];
                // rleIndex++;
                // }

                // if (rleIndex < index) {
                // if (rle_values[rleIndex] == maxPart) {
                // candidate_indices[new_length] = index_candidate;
                // new_length++;
                // }
                // }
                // }

                candidate_length = new_length;
            }
        }

        result[result_length[0]] = candidate_indices[0];
        result_length[0]++;

        return encode_pos;
    }

}
