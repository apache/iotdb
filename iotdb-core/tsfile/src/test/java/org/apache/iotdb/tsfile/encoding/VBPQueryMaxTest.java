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

public class VBPQueryMaxTest {

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
            int encode_pos, ArrayList<VBPIndexLong> indexList, long[] result, int[] result_length) {

        long min_value = bytes2Long(encoded_result, encode_pos, 8);
        encode_pos += 8;

        int bw = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        VBPIndexLong idx = indexList.get(block_index);

        int max_index = idx.findMaxIndex();

        if (max_index != -1) {
            result[result_length[0]] = idx.getCode(max_index) + min_value;
        } else {
            result[result_length[0]] = Long.MIN_VALUE;
        }

        // for (int i = 0; i < remainder; i++) {
        //     long value = idx.getCode(i) + min_value;
        //     if (result[0] < value) {
        //         result[0] = value;
        //     }
        // }

        return encode_pos;

    }

    public static void Decoder(byte[] encoded_result, ArrayList<VBPIndexLong> indexList) {
        int encode_pos = 0;

        int data_length = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int block_size = bytes2Integer(encoded_result, encode_pos, 4);
        encode_pos += 4;

        int num_blocks = data_length / block_size;

        long[] result = new long[1];
        result[0] = Long.MIN_VALUE;
        int[] result_length = new int[1];

        for (int i = 0; i < num_blocks; i++) {
            encode_pos = BlockDecoder(encoded_result, i, block_size, block_size, encode_pos, indexList, result,
                    result_length);
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
                encode_pos, indexList, result, result_length);
        // }
    }

}
