package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

//import static com.sun.org.apache.xalan.internal.lib.ExsltMath.power;

public class TSDIFFFloatTest {

    public static long combine2Int(int int1, int int2) {
        return ((long) int1 << 32) | (int2 & 0xFFFFFFFFL);
    }

    public static int getTime(long long1) {
        return ((int) (long1 >> 32));
    }

    public static int getValue(long long1) {
        return ((int) (long1));
    }

    public static int getCount(long long1, int mask) {
        return ((int) (long1 & mask));
    }
    public static int getUniqueValue(long long1, int left_shift) {
        return ((int) ((long1) >> left_shift));
    }

    public static int getBitWith(int num) {
        if (num == 0) return 1;
        else return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static void int2Bytes(int integer,int encode_pos , byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos+1] = (byte) (integer >> 16);
        cur_byte[encode_pos+2] = (byte) (integer >> 8);
        cur_byte[encode_pos+3] = (byte) (integer);
    }


    public static void intByte2Bytes(int integer, int encode_pos , byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer);
    }

    private static void long2intBytes(long integer, int encode_pos , byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos+1] = (byte) (integer >> 16);
        cur_byte[encode_pos+2] = (byte) (integer >> 8);
        cur_byte[encode_pos+3] = (byte) (integer);
    }

    public static int bytes2Integer(byte[] encoded, int start, int num) {
        int value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded[i + start] & 0xFF;
            value |= b;
        }
        return value;
    }

    private static long bytesLong2Integer(byte[] encoded, int decode_pos) {
        long value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            int b = encoded[i + decode_pos] & 0xFF;
            value |= b;
        }
        return value;
    }

    public static void pack8Values(ArrayList<Integer> values, int offset, int width, int encode_pos,  byte[] encoded_result) {
        int bufIdx = 0;
        int valueIdx = offset;
        // remaining bits for the current unfinished Integer
        int leftBit = 0;

        while (valueIdx < 8 + offset) {
            // buffer is used for saving 32 bits as a part of result
            int buffer = 0;
            // remaining size of bits in the 'buffer'
            int leftSize = 32;

            // encode the left bits of current Integer to 'buffer'
            if (leftBit > 0) {
                buffer |= (values.get(valueIdx) << (32 - leftBit));
                leftSize -= leftBit;
                leftBit = 0;
                valueIdx++;
            }

            while (leftSize >= width && valueIdx < 8 + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (values.get(valueIdx)<< (leftSize - width));
                leftSize -= width;
                valueIdx++;
            }
            // If the remaining space of the buffer can not save the bits for one Integer,
            if (leftSize > 0 && valueIdx < 8 + offset) {
                // put the first 'leftSize' bits of the Integer into remaining space of the
                // buffer
                buffer |= (values.get(valueIdx) >>> (width - leftSize));
                leftBit = width - leftSize;
            }

            // put the buffer into the final result
            for (int j = 0; j < 4; j++) {
                encoded_result[encode_pos] = (byte) ((buffer >>> ((3 - j) * 8)) & 0xFF);
                encode_pos ++;
                bufIdx++;
                if (bufIdx >= width) {
                    return ;
                }
            }
        }
//        return encode_pos;
    }

    public static void unpack8Values(byte[] encoded, int offset,int width,  ArrayList<Integer> result_list) {
        int byteIdx = offset;
        long buffer = 0;
        // total bits which have read from 'buf' to 'buffer'. i.e.,
        // number of available bits to be decoded.
        int totalBits = 0;
        int valueIdx = 0;

        while (valueIdx < 8) {
            // If current available bits are not enough to decode one Integer,
            // then add next byte from buf to 'buffer' until totalBits >= width
            while (totalBits < width) {
                buffer = (buffer << 8) | (encoded[byteIdx] & 0xFF);
                byteIdx++;
                totalBits += 8;
            }

            // If current available bits are enough to decode one Integer,
            // then decode one Integer one by one until left bits in 'buffer' is
            // not enough to decode one Integer.
            while (totalBits >= width && valueIdx < 8) {
                result_list.add ((int) (buffer >>> (totalBits - width)));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width,int encode_pos,  byte[] encoded_result) {
        int block_num = (numbers.size()-start) / 8;
        for(int i=0;i<block_num;i++){
            pack8Values( numbers, start+i*8, bit_width,encode_pos, encoded_result);
            encode_pos +=bit_width;
        }

        return encode_pos;

    }

    public static ArrayList<Integer> decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int block_size) {
        ArrayList<Integer> result_list = new ArrayList<>();
        int block_num = (block_size - 1) / 8;

        for (int i = 0; i < block_num; i++) { // bitpacking
            unpack8Values( encoded, decode_pos, bit_width,  result_list);
            decode_pos += bit_width;

        }
        return result_list;
    }


    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            int remaining,
            int[] min_delta) {
        int[] ts_block_delta = new int[remaining-1];

        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int base = i*block_size+1;
        int end = i*block_size+remaining;

        int tmp_j_1 = ts_block[base-1];
        min_delta[0] =tmp_j_1;
        int j = base;
        int tmp_j;

        while(j<end){
            tmp_j = ts_block[j];
            int epsilon_v = tmp_j - tmp_j_1;
            ts_block_delta[j-base] = epsilon_v;
            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }
            if (epsilon_v > value_delta_max) {
                value_delta_max = epsilon_v;
            }
            tmp_j_1 = tmp_j;
            j++;
        }
        j = 0;
        end = remaining -1;
        while(j<end){
            ts_block_delta[j] = ts_block_delta[j] - value_delta_min;
            j++;
        }

        min_delta[1] = value_delta_min;
        min_delta[2] = (value_delta_max-value_delta_min);


        return ts_block_delta;
    }


    public static int encodeOutlier2Bytes(
            ArrayList<Integer> ts_block_delta,
            int bit_width,
            int encode_pos,  byte[] encoded_result) {

        encode_pos = bitPacking(ts_block_delta, 0, bit_width, encode_pos, encoded_result);

        int n_k = ts_block_delta.size();
        int n_k_b = n_k / 8;
        long cur_remaining = 0; // encoded int
        int cur_number_bits = 0; // the bit width used of encoded int
        for (int i = n_k_b * 8; i < n_k; i++) {
            long cur_value = ts_block_delta.get(i);
            int cur_bit_width = bit_width; // remaining bit width of current value

            if (cur_number_bits + bit_width >= 32) {
                cur_remaining <<= (32 - cur_number_bits);
                cur_bit_width = bit_width - 32 + cur_number_bits;
                cur_remaining += ((cur_value >> cur_bit_width));
                long2intBytes(cur_remaining,encode_pos,encoded_result);
                encode_pos += 4;
                cur_remaining = 0;
                cur_number_bits = 0;
            }

            cur_remaining <<= cur_bit_width;
            cur_number_bits += cur_bit_width;
            cur_remaining += (((cur_value << (32 - cur_bit_width)) & 0xFFFFFFFFL) >> (32 - cur_bit_width)); //
        }
        cur_remaining <<= (32 - cur_number_bits);
        long2intBytes(cur_remaining,encode_pos,encoded_result);
        encode_pos += 4;
        return encode_pos;

    }


    public static ArrayList<Integer> decodeOutlier2Bytes(
            byte[] encoded,
            int decode_pos,
            int bit_width,
            int length,
            ArrayList<Integer> encoded_pos_result
    ) {

        int n_k_b = length / 8;
        int remaining = length - n_k_b * 8;
        ArrayList<Integer> result_list = new ArrayList<>(decodeBitPacking(encoded, decode_pos, bit_width, n_k_b * 8 + 1));
        decode_pos += n_k_b * bit_width;

        ArrayList<Long> int_remaining = new ArrayList<>();
        int int_remaining_size = remaining * bit_width / 32 + 1;
        for (int j = 0; j < int_remaining_size; j++) {
            int_remaining.add(bytesLong2Integer(encoded, decode_pos));
            decode_pos += 4;
        }

        int cur_remaining_bits = 32; // remaining bit width of current value
        long cur_number = int_remaining.get(0);
        int cur_number_i = 1;
        for (int i = n_k_b * 8; i < length; i++) {
            if (bit_width < cur_remaining_bits) {
                int tmp = (int) (cur_number >> (32 - bit_width));
                result_list.add(tmp);
                cur_number <<= bit_width;
                cur_number &= 0xFFFFFFFFL;
                cur_remaining_bits -= bit_width;
            } else {
                int tmp = (int) (cur_number >> (32 - cur_remaining_bits));
                int remain_bits = bit_width - cur_remaining_bits;
                tmp <<= remain_bits;

                cur_number = int_remaining.get(cur_number_i);
                cur_number_i++;
                tmp += (int) (cur_number >> (32 - remain_bits));
                result_list.add(tmp);
                cur_number <<= remain_bits;
                cur_number &= 0xFFFFFFFFL;
                cur_remaining_bits = 32 - remain_bits;
            }
        }
        encoded_pos_result.add(decode_pos);
        return result_list;
    }


    private static int BOSBlockEncoder(int[] ts_block, int block_i, int block_size, int remaining ,int encode_pos , byte[] cur_byte) {

        int[] min_delta = new int[3];
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, block_size, remaining, min_delta);

        int2Bytes(min_delta[0],encode_pos,cur_byte);
        encode_pos += 4;
        int2Bytes(min_delta[1],encode_pos,cur_byte);
        encode_pos += 4;
        int bit_width_final = getBitWith(min_delta[2]);
        intByte2Bytes(bit_width_final,encode_pos,cur_byte);
        encode_pos += 1;
        ArrayList<Integer> final_normal = new ArrayList<>();
        for(int value:ts_block_delta){
            final_normal.add(value);
        }
        encode_pos = encodeOutlier2Bytes(final_normal, bit_width_final,encode_pos,cur_byte);
        return encode_pos;
    }


    public static int BOSEncoder(
            int[] data, int block_size, byte[] encoded_result) {
        block_size++;

        int length_all = data.length;

        int encode_pos = 0;
        int2Bytes(length_all,encode_pos,encoded_result);
        encode_pos += 4;

        int block_num = length_all / block_size;
        int2Bytes(block_size,encode_pos,encoded_result);
        encode_pos+= 4;

        for (int i = 0; i < block_num; i++) {
            encode_pos =  BOSBlockEncoder(data, i, block_size, block_size,encode_pos,encoded_result);
        }

        int remaining_length = length_all - block_num * block_size;
        if (remaining_length <= 3) {
            for (int i = remaining_length; i > 0; i--) {
                int2Bytes(data[data.length - i], encode_pos, encoded_result);
                encode_pos += 4;
            }

        }
        else {

            int start = block_num * block_size;
            int remaining = length_all-start;
            encode_pos = BOSBlockEncoder(data, block_num, block_size,remaining, encode_pos,encoded_result);

        }


        return encode_pos;
    }

    public static int BOSBlockDecoder(byte[] encoded, int decode_pos, int[] value_list, int block_size, int[] value_pos_arr) {


        int value0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        value_list[value_pos_arr[0]] =value0;
        value_pos_arr[0] ++;

        int min_delta = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        ArrayList<Integer> decode_pos_normal = new ArrayList<>();
        ArrayList<Integer> final_normal = decodeOutlier2Bytes(encoded, decode_pos, bit_width_final, block_size, decode_pos_normal);

        decode_pos = decode_pos_normal.get(0);
        int normal_i = 0;
        int pre_v = value0;

        for (int i = 0; i < block_size; i++) {
            int current_delta = min_delta + final_normal.get(normal_i) ;
            pre_v = current_delta + pre_v;
            value_list[value_pos_arr[0]] = pre_v;
            value_pos_arr[0]++;
        }

        return decode_pos;
    }

    public static void BOSDecoder(byte[] encoded) {

        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;



        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;


        int[] value_list = new int[length_all+block_size];
        block_size--;

        int[] value_pos_arr = new int[1];
        for (int k = 0; k < block_num; k++) {


            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, block_size,value_pos_arr);

        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            remain_length --;
            BOSBlockDecoder(encoded, decode_pos, value_list, remain_length, value_pos_arr);
        }
    }

    @Test
    public void BOSOptimalTest() throws IOException {

        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();

        String inputDirectory = "/Users/xiaojinzhao/Documents/GitHub/elf/src/test/resources/ElfData_Short";
        String outputFile = "/Users/xiaojinzhao/Documents/GitHub/elf/src/test/resources/result.csv";
        int block_size= 1024;
        input_path_list.add(inputDirectory);
        output_path_list.add(outputFile);
        dataset_block_size.add(128);


        int repeatTime2 = 100;
//        for (int file_i = 9; file_i < 10; file_i++) {

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
//                f=tempList[2];
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Float> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();

                int max_precision = 0;
                loader.readHeaders();
                while (loader.readRecord()) {
//                        String value = loader.getValues()[index];
                    float tmp =Float.parseFloat(loader.getValues()[0]);
                    BigDecimal bigDecimal = new BigDecimal(Float.toString(tmp));

                    // 获取小数部分的精度
                    int decimalPrecision = bigDecimal.scale();
                    if(decimalPrecision > max_precision) max_precision =decimalPrecision;
                    data1.add(tmp);
//                    data2.add(Integer.valueOf(loader.getValues()[1]));
//                        data.add(Integer.valueOf(value));
                }

                int times = (int) Math.pow(10,max_precision);
                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for(int i = 0;i<data1.size();i++){
                    data2_arr[i] = (int) (data1.get(i)*times);
                }
                byte[] encoded_result = new byte[data2_arr.length*4];
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;


                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length =  BOSEncoder(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Long.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    BOSDecoder(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "TS_2DIFF",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
//                break;
            }
            writer.close();
        }
    }


}
