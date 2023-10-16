package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

import static java.lang.Math.pow;

public class RLEBOSAmortization {

    public static int getBitWith(int num) {
        if (num == 0) return 1;
        else return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static int zigzag(int num) {
        if (num < 0) return ((-num) << 1) - 1;
        else return num << 1;
    }

    public static int deZigzag(int num) {
        if (num % 2 == 0) return num >> 1;
        else return -((num + 1) >> 1);
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

    private static long bytesLong2Integer(byte[] encoded, int decode_pos, int num) {
        long value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
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
                buffer = buffer & ((1 << totalBits) - 1);
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

        for (int i = 0; i < block_num; i++) { // bitpacking  纵向8个，bit width是多少列
            unpack8Values( encoded, decode_pos, bit_width,  result_list);
            decode_pos += bit_width;
//            int[] val8 = new int[8];
//            for (int j = 0; j < 8; j++) {
//                val8[j] = 0;
//            }
//            for (int j = 0; j < bit_width; j++) {
//                byte tmp_byte = encoded.get(decode_pos + bit_width - 1 - j);
//                byte[] bit8 = new byte[8];
//                for (int k = 0; k < 8; k++) {
//                    bit8[k] = (byte) (tmp_byte & 1);
//                    tmp_byte = (byte) (tmp_byte >> 1);
//                }
//                for (int k = 0; k < 8; k++) {
//                    val8[k] = val8[k] * 2 + bit8[k];
//                }
//            }
//            for (int j = 0; j < 8; j++) {
//                result_list.add(val8[j]);
//            }
//            decode_pos += bit_width;
        }
        return result_list;
    }

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            ArrayList<Integer> min_delta,
            int supple_length,
            ArrayList<Integer> repeat_count) {
        int[] ts_block_delta = new int[ts_block.length];

//        ts_block_delta.add(ts_block.get(0));
//        System.out.println("init_size:"+ts_block.size());

        int value_delta_min = Integer.MAX_VALUE;
        for (Integer integer : ts_block) {
            if (integer < value_delta_min) value_delta_min = integer;
        }
        int repeat_i = 0;
        int pre_delta = ts_block[0] - value_delta_min;
        int pre_count = 1;
        min_delta.add(value_delta_min);
        int block_size =ts_block.length;

        int ts_block_delta_i = 0;
        for (int i = 1; i < block_size; i++) {
            int delta = ts_block[i] - value_delta_min;
            if (delta == pre_delta) {
                pre_count++;
            } else {
                if (pre_count > 7) {
                    repeat_count.add(repeat_i); // index_repeat
                    repeat_count.add(pre_count); // repeat_count
                    ts_block_delta[ts_block_delta_i]=pre_delta;
                    ts_block_delta_i ++;
                } else {
                    for (int j = 0; j < pre_count; j++){
                        ts_block_delta[ts_block_delta_i] =pre_delta;
                        ts_block_delta_i++;
                    }

                }
                pre_count = 1;
                repeat_i = i;
            }
            pre_delta = delta;
//            ts_block_delta.add(delta);
        }
        for (int j = 0; j < pre_count; j++){
            ts_block_delta[ts_block_delta_i] =pre_delta;
            ts_block_delta_i++;
        }
        min_delta.add(ts_block_delta_i);
        int[] new_ts_block_delta = new int[ts_block_delta_i];
        System.arraycopy(ts_block_delta, 0, new_ts_block_delta, 0, ts_block_delta_i);

//        System.out.println("size:"+ts_block_delta.size());
//        System.out.println(repeat_count);
        return new_ts_block_delta;
    }

    public static int[]  getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            ArrayList<Integer> min_delta,
            ArrayList<Integer> repeat_count) {
        int[] ts_block_delta = new int[ts_block.length];

//        ts_block_delta.add(ts_block.get(0));
        int value_delta_min = Integer.MAX_VALUE;
        for (int j = i*block_size; j < (i+1)*block_size; j++) {

            int integer = ts_block[j];
            if (integer < value_delta_min) value_delta_min = integer;

        }
        int pre_delta = ts_block[i*block_size]-value_delta_min;
        int pre_count = 1;
//        int block_size = ts_block.size();
        min_delta.add(value_delta_min);
        int repeat_i = 0;
        int ts_block_delta_i = 0;
        for (int j = i*block_size+1; j < (i+1)*block_size; j++) {
            int delta = ts_block[j]-value_delta_min;
            if(delta == pre_delta){
                pre_count ++;
            } else {
                if(pre_count>7){
                    repeat_count.add(repeat_i);
                    repeat_count.add(pre_count); // repeat_count
                    ts_block_delta[ts_block_delta_i]=pre_delta;
                    ts_block_delta_i ++;
                } else{
                    for (int k = 0; k < pre_count; k++){
                        ts_block_delta[ts_block_delta_i] =pre_delta;
                        ts_block_delta_i++;
                    }
                }
                pre_count =1;
                repeat_i = j - i*block_size;
            }
            pre_delta = delta;
//            ts_block_delta.add(delta);
        }
        for (int j = 0; j < pre_count; j++){
            ts_block_delta[ts_block_delta_i] =pre_delta;
            ts_block_delta_i++;
        }
        min_delta.add(ts_block_delta_i);
        int[] new_ts_block_delta = new int[ts_block_delta_i];
        System.arraycopy(ts_block_delta, 0, new_ts_block_delta, 0, ts_block_delta_i);


        return new_ts_block_delta;
    }
    public static ArrayList<Integer> getBitWith(int[] ts_block) {
        ArrayList<Integer> ts_block_bit_width = new ArrayList<>();
        for (int integers : ts_block) {
            ts_block_bit_width.add(getBitWith(integers));
        }
        return ts_block_bit_width;
    }

    public static int encode2Bytes(
            ArrayList<Integer> ts_block,
            ArrayList<Integer> min_delta,
            int bit_width,int encode_pos,  byte[] encoded_result) {

        // encode value0
        int2Bytes(min_delta.get(0),encode_pos,encoded_result);
        encode_pos += 4;

//        // encode theta
//        int2Bytes(min_delta.get(1),encode_pos,encoded_result);
//        encode_pos += 4;

        // encode value
        int2Bytes(bit_width,encode_pos,encoded_result);
        encode_pos += 4;
        encode_pos = bitPacking(ts_block, 0, bit_width,encode_pos,encoded_result);

        return encode_pos;
    }

    public static int encodeOutlier2Bytes(
            ArrayList<Integer> ts_block_delta,
            int bit_width,
            int encode_pos,  byte[] encoded_result) {
//        ArrayList<Byte> encoded_result = new ArrayList<>();
//         encode value
        encode_pos = bitPacking(ts_block_delta, 0, bit_width, encode_pos, encoded_result);
//        byte[] value_bytes = bitPacking(ts_block_delta, 0, bit_width);
//        for (byte b : value_bytes) encoded_result.add(b);

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
//                byte[] cur_remaining_byte = long2intBytes(cur_remaining);
//                for (byte b : cur_remaining_byte) encoded_result.add(b);
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
//        byte[] cur_remaining_byte = long2intBytes(cur_remaining);
//        for (byte b : cur_remaining_byte) encoded_result.add(b);


//        return encoded_result;
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
//            String binaryString = Long.toBinaryString(bytesLong2Integer(encoded, decode_pos, 4));
//            System.out.println("cur_remaining " + bytesLong2Integer(encoded, decode_pos, 4) + " 的"+binaryString.length()+"二进制表示是 " + binaryString);
            int_remaining.add(bytesLong2Integer(encoded, decode_pos, 4));
            decode_pos += 4;
        }
//        System.out.println(int_remaining);
        int cur_remaining_bits = 32; // remaining bit width of current value
        int cur_number_bits = 0;
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
                tmp += (cur_number >> (32 - remain_bits));
                result_list.add(tmp);
                cur_number <<= remain_bits;
                cur_number &= 0xFFFFFFFFL;
                cur_remaining_bits = 32 - remain_bits;
            }
        }
        encoded_pos_result.add(decode_pos);
        return result_list;
    }

    private static int BOSEncodeBits(int[] ts_block_delta,
                                     int init_block_size,
                                     int final_left_max,
                                     int final_k_start_value,
                                     int final_k_end_value,
                                     int max_delta_value,
                                     int final_alpha,
                                     ArrayList<Integer> min_delta,
                                     ArrayList<Integer> repeat_count,
                                     int encode_pos,
                                     byte[] cur_byte) {
        int block_size = ts_block_delta.length;
        // ------------------------- encode data -----------------------------------------
//        if (final_left_max == 0 && final_k_end_value == max_delta_value) {
//            cur_byte = encode2Bytes(ts_block_delta, min_delta, bit_width);
//        } else {
        ArrayList<Integer> final_left_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_right_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_left_outlier = new ArrayList<>();
        ArrayList<Integer> final_right_outlier = new ArrayList<>();
        ArrayList<Integer> final_normal = new ArrayList<>();
        int k1 = 0;
        int k2 = 0;
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();
        int index_bitmap_outlier = 0;
        int cur_index_bitmap_outlier_bits = 0;
        for (int i = 0; i < block_size; i++) {
            int cur_value = ts_block_delta[i];
            if (cur_value < final_k_start_value) {
                final_left_outlier.add(cur_value);
                final_left_outlier_index.add(i);
                if (cur_index_bitmap_outlier_bits % 8 != 7) {
                    index_bitmap_outlier <<= 2;
                    index_bitmap_outlier += 3;
                    cur_index_bitmap_outlier_bits += 2;
                } else {
                    index_bitmap_outlier <<= 1;
                    index_bitmap_outlier += 1;
                    bitmap_outlier.add(index_bitmap_outlier);
                    index_bitmap_outlier = 1;
                    cur_index_bitmap_outlier_bits = 1;
                }


                k1++;


            } else if (cur_value > final_k_end_value) {
                final_right_outlier.add(cur_value - final_k_end_value);
                final_right_outlier_index.add(i);
                if (cur_index_bitmap_outlier_bits % 8 != 7) {
                    index_bitmap_outlier <<= 2;
                    index_bitmap_outlier += 2;
                    cur_index_bitmap_outlier_bits += 2;
                } else {
                    index_bitmap_outlier <<= 1;
                    index_bitmap_outlier += 1;
                    bitmap_outlier.add(index_bitmap_outlier);
                    index_bitmap_outlier = 0;
                    cur_index_bitmap_outlier_bits = 1;
                }
                k2++;

            } else {
                final_normal.add(cur_value - final_k_start_value);
                index_bitmap_outlier <<= 1;
                cur_index_bitmap_outlier_bits += 1;
            }
            if (cur_index_bitmap_outlier_bits % 8 == 0) {
                bitmap_outlier.add(index_bitmap_outlier);
                index_bitmap_outlier = 0;
            }
        }
        if (cur_index_bitmap_outlier_bits % 8 != 0) {
//                System.out.println("index_bitmap_outlier:"+index_bitmap_outlier);

            index_bitmap_outlier <<= (8 - cur_index_bitmap_outlier_bits % 8);
//                System.out.println("index_bitmap_outlier:"+index_bitmap_outlier);
            index_bitmap_outlier &= 0xFF;
            bitmap_outlier.add(index_bitmap_outlier);
        }

        int k_byte = (k1 << 1);
        k_byte += final_alpha;
        k_byte += (k2 << 16);

        int2Bytes(k_byte,encode_pos,cur_byte);
        encode_pos += 4;

//        byte[] value0_bytes = int2Bytes(min_delta.get(0));
//        for (byte b : value0_bytes) cur_byte.add(b);
        int2Bytes(min_delta.get(0),encode_pos,cur_byte);
        encode_pos += 4;
        int size = repeat_count.size();
        intByte2Bytes(size,encode_pos,cur_byte);
        encode_pos += 1;

//        System.out.println(block_size);
        if (size != 0)
            encode_pos =encodeOutlier2Bytes(repeat_count, getBitWith(init_block_size),encode_pos,cur_byte);

//        System.out.println(size);
//        if(size != 0)
//            System.out.println(repeat_count);


        int2Bytes(final_k_start_value,encode_pos,cur_byte);
        encode_pos += 4;
        int bit_width_final = getBitWith(final_k_end_value - final_k_start_value);
        intByte2Bytes(bit_width_final,encode_pos,cur_byte);
        encode_pos += 1;
        int left_bit_width = getBitWith(final_left_max);//final_left_max
        int right_bit_width = getBitWith(max_delta_value - final_k_end_value);//final_right_min
        intByte2Bytes(left_bit_width,encode_pos,cur_byte);
        encode_pos += 1;
        intByte2Bytes(right_bit_width,encode_pos,cur_byte);
        encode_pos += 1;
        if (final_alpha == 0) { // 0
//                for (int i : bitmap) {
//                    byte[] index_bytes = intByte2Bytes(i);
//                    for (byte b : index_bytes) cur_byte.add(b);
//                }
            for (int i : bitmap_outlier) {

                intByte2Bytes(i,encode_pos,cur_byte);
                encode_pos += 1;
//                for (byte b : index_bytes) cur_byte.add(b);
            }
        } else {
            encode_pos = encodeOutlier2Bytes(final_left_outlier_index, getBitWith(block_size),encode_pos,cur_byte);
            encode_pos = encodeOutlier2Bytes(final_right_outlier_index, getBitWith(block_size),encode_pos,cur_byte);
        }
        encode_pos =encodeOutlier2Bytes(final_normal, bit_width_final,encode_pos,cur_byte);
        if (k1 != 0)
            encode_pos = encodeOutlier2Bytes(final_left_outlier, left_bit_width,encode_pos,cur_byte);
        if (k2 != 0)
            encode_pos = encodeOutlier2Bytes(final_right_outlier, right_bit_width,encode_pos,cur_byte);
        return encode_pos;

//        System.out.println("block_size:" + block_size);
//            System.out.println("k1:" + k1);
//            System.out.println("k2:" + k2);
//            System.out.println("final_alpha:" + final_alpha);
//            System.out.println("k_byte:" + k_byte);
//            System.out.println("min_delta.get(0):" + min_delta.get(0));
////            System.out.println("min_delta.get(1):" + min_delta.get(1));
//            System.out.println("final_k_start_value:" + final_k_start_value);
//        System.out.println("final_k_end_value:" + final_k_end_value);
//            System.out.println("bit_width_final:" + bit_width_final);
//            System.out.println("left_bit_width:" + left_bit_width);
//            System.out.println("right_bit_width:" + right_bit_width);
//            System.out.println("bitmap_outlier.size:" + bitmap_outlier.size());
//            if (final_alpha == 1) { //
//                System.out.println("final_left_outlier_index:" + final_left_outlier_index);
//                System.out.println("final_right_outlier_index:" + final_right_outlier_index);
//            } else {
//                System.out.println("bitmap_outlier.size():" + bitmap_outlier.size());
//                System.out.println("bitmap_outlier:" + bitmap_outlier);
//            }


//            System.out.println("n-k1-k2: " + (final_normal.size()));
//            System.out.println(bit_width_final);
//            System.out.println("k1:" + cur_k1);
//            System.out.println(left_bit_width);
//            System.out.println("k2:" + (cur_k-cur_k1));
//            System.out.println(right_bit_width);
//            System.out.println(cur_byte.size());
//
//        System.out.println("max_delta_value:     " + max_delta_value);
//        int cur_bits = 0;
//            cur_bits +=  (Math.min((k1+k2) * getBitWith(block_size), block_size + k1+k2));
//            System.out.println("cur_bits:     " + cur_bits);
//            cur_bits += cur_k1 * left_bit_width;//left_max
//            cur_bits += (block_size - cur_k ) * bit_width_final;
//            cur_bits += (cur_k- cur_k1) * right_bit_width;//left_max
//            System.out.println("cur_bits:     " + cur_bits);


        // correct
//            System.out.println("final_left_outlier.size():"+final_left_outlier.size());
//            System.out.println("k1:"+k1);
//            System.out.println("final_right_outlier.size():"+final_right_outlier.size());
//            System.out.println("k2:"+k2);
//        cur_byte.addAll(encodeOutlier2Bytes(final_normal, bit_width_final));
//        if (k1 != 0)
//            cur_byte.addAll(encodeOutlier2Bytes(final_left_outlier, left_bit_width));
//        if (k2 != 0)
//            cur_byte.addAll(encodeOutlier2Bytes(final_right_outlier, right_bit_width));
//            System.out.println(final_normal);
//        System.out.println("final_left_outlier_index:" + final_left_outlier_index);
//        System.out.println(final_left_outlier);
//        System.out.println("final_right_outlier_index:" + final_right_outlier_index);
//            System.out.println(final_right_outlier);
    }
//    }
    private static int BOSBlockEncoder(int[] ts_block, int supple_length,  int k,int q, int encode_pos, byte[] cur_byte) {

    int block_size = ts_block.length;
    ArrayList<Integer> repeat_count = new ArrayList<>();
    ArrayList<Integer> min_delta = new ArrayList<>();
    int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, min_delta,supple_length,repeat_count);

    int init_block_size = ts_block.length;
    block_size = min_delta.get(1);

    int final_right_max = 0;
    double sum = 0;
    double sumOfSquares = 0;

    for (int i = 0; i < block_size; i++) {
        int delta = ts_block_delta[i];
        if (delta > final_right_max) {
            final_right_max = delta;
        }
        sum += delta;
        sumOfSquares += delta * delta;
    }

    double mu = sum / block_size;
    double variance = (sumOfSquares / block_size) - (mu * mu);
    double sigma = Math.sqrt(variance);

    if(sigma ==0){
        encode_pos=  BOSEncodeBits(ts_block_delta, init_block_size,0, 0, 0, final_right_max,
              1,  min_delta, repeat_count,encode_pos, cur_byte);
        return encode_pos;
    }


        final_right_max = ((final_right_max + 1)/q +1)*q;
//        int bit_width = getBitWith(ts_block_delta.get(block_size - 1));
        int final_up_bound = ((int) ((mu + k * sigma) / q + 1) * q);

        int max_delta_value_bit_width = getBitWith(final_up_bound);

    int final_k_start_value = 0;
    int final_k_end_value = final_right_max;
    ArrayList<Integer> spread_value = new ArrayList<>();
    for (int i =(int) Math.floor(Math.log(sigma)); i < max_delta_value_bit_width; i++) {//
        int spread_v = (int) pow(2, i) - 1;
        spread_value.add(spread_v);
    }

    int min_bits = 0;
    min_bits += (getBitWith(final_k_end_value - final_k_start_value) * (block_size ));

    // Divided into buckets
    int bucket_num = (int) Math.ceil (((double) final_right_max +1) / (double) q);
        int[] bucket_count = new int[bucket_num];
        for(int i=0;i<bucket_num;i++){
            bucket_count[i]=0;
        }
        for (int i = 0; i < block_size; i++) {
            int tmp_i = ts_block_delta[i]/q;
            bucket_count[tmp_i]  ++;
        }
        int[] PDF = new int[bucket_num];
        int cumulative = 0;
        int bucket_start_i = (int) (Math.max((mu-k*sigma)/q + 1,0));//0;
        int bucket_end_i = (int) (Math.min((mu+k*sigma)/q - 1,bucket_num));//bucket_num;
        for(int i=0;i<bucket_num;i++){
            int count = bucket_count[i];
            PDF[i] = cumulative;
            cumulative += count;
        }
        PDF[bucket_num-1] = cumulative;
        int PDF_size = bucket_num;

        int k1=0;
        int k2=0;
    for(int i =bucket_start_i;i<bucket_end_i;i++){
        if(bucket_count[i] == 0){
            continue;
        }
        int k_start_value = i * q;
        for (int k_spread_value : spread_value) {
            int k_end_value = Math.min(k_spread_value + k_start_value, final_right_max);

            int cur_bits = 0;
            int cur_k1 = PDF[i];

            int k_end_value_bucket = k_end_value/q ;
            k_end_value = k_end_value_bucket *q;

            int cur_k2 = 0;
            if(k_end_value_bucket != PDF_size -1)
                cur_k2 =  block_size - PDF[k_end_value_bucket+1];
            else cur_k2 =  block_size - PDF[k_end_value_bucket];


            cur_bits += Math.min((cur_k1 + cur_k2) * getBitWith(block_size), block_size + cur_k1 + cur_k2);
            if (cur_k1 != 0)
                cur_bits += cur_k1 * getBitWith(k_start_value);//left_max
            if (cur_k1 + cur_k2 != block_size)
                cur_bits += (block_size - cur_k1 - cur_k2) * getBitWith(k_end_value - k_start_value);
            if (cur_k2 != 0)
                cur_bits += cur_k2 * getBitWith(final_right_max - k_end_value);//min_upper_outlier


            if (cur_bits < min_bits) {
                min_bits = cur_bits;
                k1 = cur_k1;
                k2 = cur_k2;
                final_k_start_value = k_start_value;
                final_k_end_value = k_end_value;
            }
            if (k_end_value == final_right_max)
                break;
        }
    }

    int final_alpha = ((k1 + k2) * getBitWith(block_size)) <= (block_size + k1 + k2) ? 1 : 0;

        encode_pos = BOSEncodeBits(ts_block_delta,init_block_size, final_k_start_value, final_k_start_value, final_k_end_value, final_right_max,
            final_alpha,   min_delta, repeat_count,encode_pos, cur_byte);

    return encode_pos;
}

    private static int BOSBlockEncoder(int[] ts_block, int block_i, int block_size, int k, int q, int encode_pos , byte[] cur_byte) {


        ArrayList<Integer> repeat_count = new ArrayList<>();
        int init_block_size = block_size;
        ArrayList<Integer> min_delta = new ArrayList<>();

        int[]  ts_block_delta = getAbsDeltaTsBlock(ts_block,block_i, init_block_size, min_delta, repeat_count);
        block_size = min_delta.get(1);
//        ArrayList<Integer> ts_block_delta = getAbsDeltaTsBlock(ts_block, min_delta);

//        block_size --;

        int final_right_max = 0;
        double sum = 0;
        double sumOfSquares = 0;

        for (int i = 0; i < block_size; i++) {
            int delta = ts_block_delta[i];
            if (delta > final_right_max) {
                final_right_max = delta;
            }
            sum += delta;
            sumOfSquares += delta * delta;
        }

        double mu = sum / block_size;
        double variance = (sumOfSquares / block_size) - (mu * mu);
        double sigma = Math.sqrt(variance);

        if(sigma ==0){
            encode_pos=  BOSEncodeBits(ts_block_delta, init_block_size,0, 0, 0, final_right_max,
                    1,  min_delta, repeat_count,encode_pos, cur_byte);
            return encode_pos;
        }


        final_right_max = ((final_right_max + 1)/q +1)*q;
//        int bit_width = getBitWith(ts_block_delta.get(block_size - 1));
        int final_up_bound = ((int) ((mu + k * sigma) / q + 1) * q);

        int max_delta_value_bit_width = getBitWith(final_up_bound);

        int final_k_start_value = 0;
        int final_k_end_value = final_right_max;
        ArrayList<Integer> spread_value = new ArrayList<>();
        for (int i =(int) Math.floor(Math.log(sigma)); i < max_delta_value_bit_width; i++) {//
            int spread_v = (int) pow(2, i) - 1;
            spread_value.add(spread_v);
        }

        int min_bits = 0;
        min_bits += (getBitWith(final_k_end_value - final_k_start_value) * (block_size ));

        // Divided into buckets
        int bucket_num = (int) Math.ceil (((double) final_right_max +1) / (double) q);
        int[] bucket_count = new int[bucket_num];
        for(int i=0;i<bucket_num;i++){
            bucket_count[i]=0;
        }
        for (int i = 0; i < block_size; i++) {
            int tmp_i = ts_block_delta[i]/q;
            bucket_count[tmp_i]  ++;
        }
        int[] PDF = new int[bucket_num];
        int cumulative = 0;
        int bucket_start_i = (int) (Math.max((mu-k*sigma)/q + 1,0));//0;
        int bucket_end_i = (int) (Math.min((mu+k*sigma)/q - 1,bucket_num));//bucket_num;
        for(int i=0;i<bucket_num;i++){
            int count = bucket_count[i];
            PDF[i] = cumulative;
            cumulative += count;
        }
        PDF[bucket_num-1] = cumulative;
        int PDF_size = bucket_num;

        int k1=0;
        int k2=0;
        for(int i =bucket_start_i;i<bucket_end_i;i++){
            if(bucket_count[i] == 0){
                continue;
            }
            int k_start_value = i * q;
            for (int k_spread_value : spread_value) {
                int k_end_value = Math.min(k_spread_value + k_start_value, final_right_max);

                int cur_bits = 0;
                int cur_k1 = PDF[i];

                int k_end_value_bucket = k_end_value/q ;
                k_end_value = k_end_value_bucket *q;

                int cur_k2 = 0;
                if(k_end_value_bucket != PDF_size -1)
                    cur_k2 =  block_size - PDF[k_end_value_bucket+1];
                else cur_k2 =  block_size - PDF[k_end_value_bucket];


                cur_bits += Math.min((cur_k1 + cur_k2) * getBitWith(block_size), block_size + cur_k1 + cur_k2);
                if (cur_k1 != 0)
                    cur_bits += cur_k1 * getBitWith(k_start_value);//left_max
                if (cur_k1 + cur_k2 != block_size)
                    cur_bits += (block_size - cur_k1 - cur_k2) * getBitWith(k_end_value - k_start_value);
                if (cur_k2 != 0)
                    cur_bits += cur_k2 * getBitWith(final_right_max - k_end_value);//min_upper_outlier


                if (cur_bits < min_bits) {
                    min_bits = cur_bits;
                    k1 = cur_k1;
                    k2 = cur_k2;
                    final_k_start_value = k_start_value;
                    final_k_end_value = k_end_value;
                }
                if (k_end_value == final_right_max)
                    break;
            }
        }

        int final_alpha = ((k1 + k2) * getBitWith(block_size)) <= (block_size + k1 + k2) ? 1 : 0;

        encode_pos = BOSEncodeBits(ts_block_delta,init_block_size, final_k_start_value, final_k_start_value, final_k_end_value, final_right_max,
                final_alpha,   min_delta, repeat_count,encode_pos, cur_byte);

        return encode_pos;
    }


    public static int BOSEncoder(
            int[] data, int block_size, String dataset_name, int k,int q,byte[] encoded_result) throws IOException {
//        block_size++;
        int length_all = data.length;

        int encode_pos = 0;
        int2Bytes(length_all,encode_pos,encoded_result);
        encode_pos += 4;
//        byte[] length_all_bytes = int2Bytes(length_all);
//        for (byte b : length_all_bytes) encoded_result.add(b);
        int block_num = length_all / block_size;
        int2Bytes(block_size,encode_pos,encoded_result);
        encode_pos+= 4;
//        byte[] block_size_byte = int2Bytes(block_size);
//        for (byte b : block_size_byte) encoded_result.add(b);


//        for (int i = 0; i < 1; i++) {
        for (int i = 0; i < block_num; i++) {

            encode_pos =  BOSBlockEncoder(data, i, block_size, k,q,encode_pos,encoded_result);

//            ArrayList<Byte> cur_encoded_result = BOSBlockEncoder(data, i, block_size,0,q,k);
//            encoded_result.addAll(cur_encoded_result);

        }

        int remaining_length = length_all - block_num * block_size;
        if (remaining_length <= 3) {
            for (int i = remaining_length; i > 0; i--) {
                int2Bytes(data[data.length - i], encode_pos, encoded_result);
                encode_pos += 4;
            }

        } else {

            int start = block_num * block_size;
            int[] ts_block = new int[length_all-start];
            if (length_all - start >= 0) System.arraycopy(data, start, ts_block, start - start, length_all - start);

            int supple_length;
            if (remaining_length % 8 == 0) {
                supple_length = 1;
            } else if (remaining_length % 8 == 1) {
                supple_length = 0;
            } else {
                supple_length = 9 - remaining_length % 8;
            }


            encode_pos = BOSBlockEncoder(ts_block, supple_length, k,q,encode_pos,encoded_result);
//            encoded_result.addAll(cur_encoded_result);
//            System.out.println("encoded_result.size: "+cur_encoded_result.size());
        }


        return encode_pos;
    }

    public static int BOSBlockDecoder(byte[] encoded, int decode_pos, int[] value_list, int block_size, int[] value_pos_arr) {

        int k_byte = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int k1_byte = (int) (k_byte % pow(2, 16));
        int k1 = k1_byte / 2;
        int final_alpha = k1_byte % 2;

        int k2 = (int) (k_byte / pow(2, 16));


        int min_delta = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int count_size = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;
//        System.out.println(count_size);
        ArrayList<Integer> repeat_count = new ArrayList<>();
        if (count_size != 0) {
            ArrayList<Integer> repeat_count_result = new ArrayList<>();
            repeat_count = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(block_size), count_size, repeat_count_result);
            decode_pos = repeat_count_result.get(0);
//            System.out.println(repeat_count);
        }
//        decode_pos

        int cur_block_size = block_size;
        for (int i = 1; i < count_size; i += 2) {
            cur_block_size -= (repeat_count.get(i) - 1);
        }

        int final_k_start_value = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        int left_bit_width = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;
        int right_bit_width = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;
//System.out.println("blocksize:"+block_size);
        ArrayList<Integer> final_left_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_right_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_left_outlier = new ArrayList<>();
        ArrayList<Integer> final_right_outlier = new ArrayList<>();
        ArrayList<Integer> final_normal = new ArrayList<>();
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();

//        System.out.println("k1:" + k1);
//        System.out.println("k2:" + k2);
//        System.out.println("cur_block_size:" + cur_block_size);

        if (final_alpha == 0) { // 0
            int bitmap_bytes = (int) Math.ceil((double) (cur_block_size + k1 + k2) / (double) 8);
//            System.out.println("bitmap_bytes:" + bitmap_bytes);
            for (int i = 0; i < bitmap_bytes; i++) {
                bitmap_outlier.add(bytes2Integer(encoded, decode_pos, 1));
                decode_pos += 1;
            }
            int bitmap_outlier_i = 0;
            int remaining_bits = 8;
            int tmp = bitmap_outlier.get(bitmap_outlier_i);
            bitmap_outlier_i++;
            int i = 0;
            while (i < cur_block_size) {
                if (remaining_bits > 1) {
                    int bit_i = (tmp >> (remaining_bits - 1)) & 0x1;
                    remaining_bits -= 1;
                    if (bit_i == 1) {
                        int bit_left_right = (tmp >> (remaining_bits - 1)) & 0x1;
                        remaining_bits -= 1;
                        if (bit_left_right == 1) {
                            final_left_outlier_index.add(i);
                        } else {
                            final_right_outlier_index.add(i);
                        }
                    }
                    if (remaining_bits == 0) {
                        remaining_bits = 8;
                        if (bitmap_outlier_i >= bitmap_bytes) break;
//                        System.out.println("bitmap_outlier_i:"+bitmap_outlier_i);
//                        System.out.println("i:"+i);
                        tmp = bitmap_outlier.get(bitmap_outlier_i);
                        bitmap_outlier_i++;
                    }
                } else if (remaining_bits == 1) {
                    int bit_i = tmp & 0x1;
                    remaining_bits = 8;
                    if (bitmap_outlier_i >= bitmap_bytes) break;
                    tmp = bitmap_outlier.get(bitmap_outlier_i);
                    bitmap_outlier_i++;
                    if (bit_i == 1) {
                        int bit_left_right = (tmp >> (remaining_bits - 1)) & 0x1;
                        remaining_bits -= 1;
                        if (bit_left_right == 1) {
                            final_left_outlier_index.add(i);
                        } else {
                            final_right_outlier_index.add(i);
                        }
                    }
                }
                i++;
            }
        } else {

//            if(k1 != 0){
            ArrayList<Integer> decode_pos_result_left = new ArrayList<>();
            final_left_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(cur_block_size), k1, decode_pos_result_left);
            decode_pos = (decode_pos_result_left.get(0));
//            }
//            if(k2 != 0){
            ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
            final_right_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(cur_block_size), k2, decode_pos_result_right);
            decode_pos = (decode_pos_result_right.get(0));
//            }
        }
//        System.out.println("cur_block_size:" + cur_block_size);
//            System.out.println("k1:" + k1);
//            System.out.println("k2:" + k2);
//            System.out.println("final_alpha:" + final_alpha);
//            System.out.println("k_byte:" + k_byte);
////            System.out.println("value0:" + value0);
//            System.out.println("min_delta.get(1):" + min_delta);
//            System.out.println("final_k_start_value:" + final_k_start_value);
//            System.out.println("bit_width_final:" + bit_width_final);
//            System.out.println("left_bit_width:" + left_bit_width);
//            System.out.println("right_bit_width:" + right_bit_width);
//            if (final_alpha == 1) { //1
//                System.out.println("final_left_outlier_index:" + final_left_outlier_index);
//                System.out.println("final_right_outlier_index:" + final_right_outlier_index);
//            } else {
//                System.out.println("bitmap_outlier.size():" + bitmap_outlier.size());
//                System.out.println("bitmap_outlier:" + bitmap_outlier);
//            }


        ArrayList<Integer> decode_pos_normal = new ArrayList<>();
        final_normal = decodeOutlier2Bytes(encoded, decode_pos, bit_width_final, cur_block_size - k1 - k2, decode_pos_normal);
//            System.out.println(final_normal.size());
        decode_pos = decode_pos_normal.get(0);
        if (k1 != 0) {
            ArrayList<Integer> decode_pos_result_left = new ArrayList<>();
            final_left_outlier = decodeOutlier2Bytes(encoded, decode_pos, left_bit_width, k1, decode_pos_result_left);
//            System.out.println("final_left_outlier_index:"+final_left_outlier_index);
//            System.out.println(final_left_outlier);
//            System.out.println("final_left_outlier_index:"+final_left_outlier_index.size());
//            System.out.println("k1:"+k1);
//            System.out.println("final_left_outlier:"+final_left_outlier.size());
            decode_pos = decode_pos_result_left.get(0);
        }
        if (k2 != 0) {
            ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
            final_right_outlier = decodeOutlier2Bytes(encoded, decode_pos, right_bit_width, k2, decode_pos_result_right);
//            System.out.println("final_right_outlier_index:"+final_right_outlier_index);
//                System.out.println(final_right_outlier);
//            System.out.println("final_right_outlier_size:"+final_right_outlier_index.size());
//            System.out.println("k2:"+k2);
//            System.out.println("final_right_outlier:"+final_right_outlier.size());
            decode_pos = decode_pos_result_right.get(0);
        }
        int left_outlier_i = 0;
        int right_outlier_i = 0;
        int normal_i = 0;
        int pre_v = 0;
        int final_k_end_value = (int) (final_k_start_value + pow(2, bit_width_final));

        int cur_i = 0;
        int repeat_i = 0;
        for (int i = 0; i < cur_block_size; i++) {

            int current_delta;
            if (left_outlier_i >= k1) {
                if (right_outlier_i >= k2) {
                    current_delta = final_normal.get(normal_i) + final_k_start_value;
                    normal_i++;
                } else if (i == final_right_outlier_index.get(right_outlier_i)) {
                    current_delta = final_right_outlier.get(right_outlier_i) + final_k_end_value;
                    right_outlier_i++;
                } else {
                    current_delta = final_normal.get(normal_i) + final_k_start_value;
                    normal_i++;
                }
            } else if (i == final_left_outlier_index.get(left_outlier_i)) {
                current_delta = final_left_outlier.get(left_outlier_i);
                left_outlier_i++;
            } else {

                if (right_outlier_i >= k2) {
                    current_delta = final_normal.get(normal_i) + final_k_start_value;
                    normal_i++;
                } else if (i == final_right_outlier_index.get(right_outlier_i)) {
                    current_delta = final_right_outlier.get(right_outlier_i) + final_k_end_value;
                    right_outlier_i++;
                } else {
                    current_delta = final_normal.get(normal_i) + final_k_start_value;
                    normal_i++;
                }
            }
            pre_v = current_delta + min_delta;
            if (repeat_i < count_size) {
                if (cur_i == repeat_count.get(repeat_i)) {
                    cur_i += (repeat_count.get(repeat_i+1));

                    for (int j = 0; j < repeat_count.get(repeat_i + 1); j++) {
                        value_list[value_pos_arr[0]] = pre_v;
                        value_pos_arr[0]++;
                    }
                    repeat_i += 2;
                }else {
                    cur_i++;
                    value_list[value_pos_arr[0]] = pre_v;
                    value_pos_arr[0]++;
                }
            } else {
                cur_i++;
                value_list[value_pos_arr[0]] = pre_v;
                value_pos_arr[0]++;
            }
        }
//        System.out.println(cur_i);
//        System.out.println(value_list.size());
        return decode_pos;
    }

    public static int[] BOSDecoder(byte[] encoded) {

        int decode_pos = 0;
        int length_all = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int block_num = length_all / block_size;
        int remain_length = length_all - block_num * block_size;
        int zero_number;
        if (remain_length % 8 == 0) {
            zero_number = 0;
        } else {
            zero_number = 8 - remain_length % 8;
        }
        int[] value_list = new int[length_all+8];
        int[] value_pos_arr = new int[1];
        value_pos_arr[0]= 0;
//        for (int k = 0; k < 1; k++) {
        for (int k = 0; k < block_num; k++) {
//            System.out.println(k);
            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, block_size,value_pos_arr);
//            System.out.println(value_list);
        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, remain_length + zero_number,value_pos_arr);
        }
        return value_list;
    }

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
        String parent_dir = "iotdb/iotdb-core/tsfile/src/test/resources/"; // your data path
        String output_parent_dir = parent_dir + "rle_bos_amortization";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        dataset_name.add("CS-Sensors");
        dataset_name.add("Metro-Traffic");
        dataset_name.add("USGS-Earthquakes");
        dataset_name.add("YZ-Electricity");
        dataset_name.add("GW-Magnetic");
        dataset_name.add("TY-Fuel");
        dataset_name.add("Cyber-Vehicle");
        dataset_name.add("Vehicle-Charge");
        dataset_name.add("Nifty-Stocks");
        dataset_name.add("TH-Climate");
        dataset_name.add("TY-Transport");
        dataset_name.add("EPM-Education");

        for (int i = 0; i < dataset_name.size(); i++) {
            input_path_list.add(input_parent_dir + dataset_name.get(i));
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
        dataset_block_size.add(1024);

//        String parent_dir = "C:\\Users\\Jinnsjao Shawl\\Documents\\GitHub\\encoding-outlier\\";
//        String output_parent_dir = parent_dir + "vldb\\compression_ratio\\outlier";
//        String input_parent_dir = parent_dir + "iotdb_test_small\\";
//        ArrayList<String> input_path_list = new ArrayList<>();
//        ArrayList<String> output_path_list = new ArrayList<>();
//        ArrayList<String> dataset_name = new ArrayList<>();
//        ArrayList<Integer> dataset_block_size = new ArrayList<>();
//        dataset_name.add("CS-Sensors");
//        dataset_name.add("Metro-Traffic");
//        dataset_name.add("USGS-Earthquakes");
//        dataset_name.add("YZ-Electricity");
//        dataset_name.add("GW-Magnetic");
//        dataset_name.add("TY-Fuel");
//        dataset_name.add("Cyber-Vehicle");
//        dataset_name.add("Vehicle-Charge");
//        dataset_name.add("Nifty-Stocks");
//        dataset_name.add("TH-Climate");
//        dataset_name.add("TY-Transport");
//        dataset_name.add("EPM-Education");
//
//        for (int i = 0; i < dataset_name.size(); i++) {
//            input_path_list.add(input_parent_dir + dataset_name.get(i));
//        }
//
//        output_path_list.add(output_parent_dir + "\\CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
//        output_path_list.add(output_parent_dir + "\\Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(256);
//        output_path_list.add(output_parent_dir + "\\GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(128);
//        output_path_list.add(output_parent_dir + "\\TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(64);
//        output_path_list.add(output_parent_dir + "\\Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(128);
//        output_path_list.add(output_parent_dir + "\\Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(256);
//        output_path_list.add(output_parent_dir + "\\TH-Climate_ratio.csv");//9
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\TY-Transport_ratio.csv");//10
//        dataset_block_size.add(512);
//        output_path_list.add(output_parent_dir + "\\EPM-Education_ratio.csv");//11
//        dataset_block_size.add(512);


        ArrayList<Integer> columnIndexes = new ArrayList<>(); // set the column indexes of compressed
        for (int i = 0; i < 2; i++) {
            columnIndexes.add(i, i);
        }

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
//        for (int file_i = 0; file_i < 1; file_i++) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            int repeatTime = 1; // set repeat time

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
//                f = tempList[2];
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();
                int[] data_decoded ;


//                for (int index : columnIndexes) {
                // add a column to "data"
//                    System.out.println(index);

                loader.readHeaders();
//                    data.clear();
                while (loader.readRecord()) {
//                        String value = loader.getValues()[index];
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
//                        data.add(Integer.valueOf(value));
                }
//                    System.out.println(data2);
                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for(int i = 0;i<data2.size();i++){
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length*4];
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;
                int repeatTime2 = 500;

//                    ArrayList<Byte> buffer2 = new ArrayList<>();
                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length =  BOSEncoder(data2_arr, dataset_block_size.get(file_i), dataset_name.get(file_i),  1,4,encoded_result);
                }
//                        System.out.println(buffer2.size());
//                            buffer_bits = ReorderingRegressionEncoder(data, dataset_block_size.get(file_i), dataset_name.get(file_i));

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = (double) compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    data_decoded = BOSDecoder(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "RLE+BOS-A",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);

//              break;

//
            }
            writer.close();

        }
    }

}
