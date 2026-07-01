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
import java.util.BitSet;
import java.util.HashMap;

public class VBPQuerySmallerPartsTest {

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;

        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
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

    public static void BlockDecoder(byte[] encoded_result1, byte[] encoded_result2, int block_index, int block_size1, int block_size2,
            int[] encode_pos, ArrayList<VBPIndexLong> indexList1, ArrayList<VBPIndexLong> indexList2, int[] result, int[] result_length,
            int bound_query_range) {

        long min_value1 = bytes2Long(encoded_result1, encode_pos[0], 8);
        encode_pos[0] += 8;

        int bw1 = bytes2Integer(encoded_result1, encode_pos[0], 4);
        encode_pos[0] += 4;

        long min_value2 = bytes2Long(encoded_result2, encode_pos[1], 8);
        encode_pos[1] += 8;

        int bw2 = bytes2Integer(encoded_result2, encode_pos[1], 4);
        encode_pos[1] += 4;


        VBPIndexLong idx1 = indexList1.get(block_index);
        VBPIndexLong idx2 = indexList2.get(block_index);

        // BitSet bitset_result1 = idx1.select(VBPIndexLong.Op.LT, bound_query_range);
        // BitSet bitset_result2 = idx2.select(VBPIndexLong.Op.LT, bound_query_range);

        // for (int i = 0; i < bitset_result1.length(); i++) {
        //     if (bitset_result1.get(i) && bitset_result2.get(i)) {
        //         result[result_length[0]] = i + (block_index * block_size1);
        //         result_length[0]++;
        //     }
        // }

        int[] query_result1 = idx1.select(VBPIndexLong.Op.LT, bound_query_range);
        int[] query_result2 = idx2.select(VBPIndexLong.Op.LT, bound_query_range);

        int i = 0, j = 0;
        while (i < query_result1.length && j < query_result2.length) {
            if (query_result1[i] == query_result2[j]) {
                result[result_length[0]] = query_result1[i] + (block_index * block_size1);
                result_length[0]++;
                i++;
                j++;
            } else if (query_result1[i] < query_result2[j]) {
                i++;
            } else {
                j++;
            }
        }

    }

    public static void Decoder(byte[] encoded_result1, byte[] encoded_result2, ArrayList<VBPIndexLong> indexList1, ArrayList<VBPIndexLong> indexList2, int bound_query_range) {
        int[] encode_pos = new int[2];

        int data_length1 = bytes2Integer(encoded_result1, encode_pos[0], 4);
        encode_pos[0] += 4;

        int block_size1 = bytes2Integer(encoded_result1, encode_pos[0], 4);
        encode_pos[0] += 4;

        int num_blocks = data_length1 / block_size1;

        int data_length2 = bytes2Integer(encoded_result2, encode_pos[1], 4);
        encode_pos[1] += 4;

        int block_size2 = bytes2Integer(encoded_result2, encode_pos[1], 4);
        encode_pos[1] += 4;


        int[] result = new int[data_length1];
        int[] result_length = new int[1];

        for (int i = 0; i < num_blocks; i++) {
            BlockDecoder(encoded_result1, encoded_result2, i, block_size1, block_size2, encode_pos, indexList1, indexList2, result,
                    result_length, bound_query_range);
        }

        int remainder = data_length1 % block_size1;

        // if (remainder <= 3) {
        // for (int i = 0; i < remainder; i++) {
        // data[num_blocks * block_size + i] = bytes2Long(encoded_result, encode_pos,
        // 8);
        // encode_pos += 8;
        // }
        // } else {
        BlockDecoder(encoded_result1, encoded_result2, num_blocks, block_size1, block_size2,
                encode_pos, indexList1, indexList2, result, result_length, bound_query_range);
        // }
    }

}
