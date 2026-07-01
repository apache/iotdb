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

public class SubcolumnQueryCount2Test {

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
            encode_pos = BlockQuery(encoded_result, i, block_size,
                    block_size, encode_pos,
                    result, result_length);
        }

        int remainder = data_length % block_size;

        if (remainder <= 3) {
            for (int i = 0; i < remainder; i++) {
                // int value = ((encoded_result[encode_pos] & 0xFF) << 24) |
                //         ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                //         ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
                int value = SubcolumnTest.bytes2Integer(encoded_result, encode_pos, 4);
                encode_pos += 4;
                result[result_length[0]]++;
            }
        } else {
            encode_pos = BlockQuery(encoded_result, num_blocks, block_size,
                    remainder, encode_pos,
                    result, result_length);
        }

    }

    public static int BlockQuery(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, int[] result, int[] result_length) {
        int[] min_delta = new int[3];

        min_delta[0] = ((encoded_result[encode_pos] & 0xFF) << 24) | ((encoded_result[encode_pos + 1] & 0xFF) << 16) |
                ((encoded_result[encode_pos + 2] & 0xFF) << 8) | (encoded_result[encode_pos + 3] & 0xFF);
        encode_pos += 4;

        // int[] block_data = new int[remainder];

        int m = encoded_result[encode_pos];
        encode_pos += 1;

        if (m == 0) {
            result[result_length[0]] += remainder;
            return encode_pos;
        }

        int bw = SubcolumnTest.bitWidth(block_size);

        int beta = encoded_result[encode_pos];
        encode_pos += 1;

        int l = (m + beta - 1) / beta;

        int[] bitWidthList = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 8, l, bitWidthList);

        // int[][] subcolumnList = new int[l][remainder];

        int[] encodingType = new int[l];

        encode_pos = SubcolumnTest.decodeBitPacking(encoded_result, encode_pos, 1, l, encodingType);

        for (int i = l - 1; i >= 0; i--) {
            int type = encodingType[i];
            if (type == 0) {
                encode_pos *= 8;
                encode_pos += bitWidthList[i] * remainder;
                encode_pos = (encode_pos + 7) / 8;
            } else {
                int index = ((encoded_result[encode_pos] & 0xFF) << 8) | (encoded_result[encode_pos + 1] & 0xFF);
                encode_pos += 2;

                // encode_pos = (encode_pos * 8 + bw * index + 7) / 8;
                // encode_pos = (encode_pos * 8 + bitWidthList[i] * index + 7) / 8;

                encode_pos *= 8;
                encode_pos += bw * index;
                encode_pos = (encode_pos + 7) / 8;

                encode_pos *= 8;
                encode_pos += bitWidthList[i] * index;
                encode_pos = (encode_pos + 7) / 8;
                
            }
        }

        // if (target <= 0) {
        // for (int i = 0; i < remainder; i++) {
        // result[result_length[0]] = block_size * block_index + i;
        // result_length[0]++;
        // }
        // return encode_pos;
        // }

        result[result_length[0]] += remainder;

        return encode_pos;
    }

}
