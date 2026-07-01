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

public class VBPQueryGreaterLessTest {

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

    public static int BlockDecoder(byte[] encoded_result, int block_index, int block_size, int remainder,
            int encode_pos, ArrayList<VBPIndexLong> indexList, int[] result, int[] result_length,
            int bound_query_range, int bound_query_less_range) {

        long min_value = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        int bw = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        VBPIndexLong idx = indexList.get(block_index);

        // BitSet bitset_result = idx.select(VBPIndexLong.Op.GT, bound_query_range);

        // BitSet bitset_result_less = idx.select(VBPIndexLong.Op.LT,
        // bound_query_less_range);

        // for (int i = 0; i < bitset_result.length(); i++) {
        // if (bitset_result.get(i) && bitset_result_less.get(i)) {
        // result[result_length[0]] = i + (block_index * block_size);
        // result_length[0]++;
        // }
        // }

        int[] query_result = idx.select(VBPIndexLong.Op.GT, bound_query_range);
        int[] query_result_less = idx.select(VBPIndexLong.Op.LT, bound_query_less_range);

        int i = 0, j = 0;

        while (i < query_result.length && j < query_result_less.length) {
            if (query_result[i] == query_result_less[j]) {
                result[result_length[0]] = query_result[i] + (block_index * block_size);
                result_length[0]++;
                i++;
                j++;
            } else if (query_result[i] < query_result_less[j]) {
                i++;
            } else {
                j++;
            }
        }

        return encode_pos;

    }

    public static void Decoder(byte[] encoded_result, ArrayList<VBPIndexLong> indexList, int bound_query_range,
            int bound_query_less_range) {
        int encode_pos = 0;

        int data_length = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int block_size = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        int[] result = new int[data_length];
        int[] result_length = new int[1];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, encode_pos, indexList, result,
                    result_length, bound_query_range, bound_query_less_range);
        }

        int remainder = data_length % block_size;

        // if (remainder <= 3) {
        // for (int i = 0; i < remainder; i++) {
        // data[num_blocks * block_size + i] = bytes2Long(encoded_result, encode_pos,
        // 8);
        // encode_pos += 8;
        // }
        // } else {
        encode_pos = BlockDecoder(encoded_result, num_blocks, block_size, remainder,
                encode_pos, indexList, result, result_length, bound_query_range, bound_query_less_range);
        // }
    }

}
