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

import static java.lang.Math.pow;

public class PruneBosTestD {

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
            int[] min_delta,
            int supple_length) {
        int block_size = ts_block.length-1;
        int[] ts_block_delta = new int[ts_block.length+supple_length-1];


        int value_delta_min = Integer.MAX_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        for (int i = 0; i < block_size; i++) {

            int epsilon_v = ts_block[i+1] - ts_block[i];

            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }
            if (epsilon_v > value_delta_max) {
                value_delta_max = epsilon_v;
            }

        }
        min_delta[0] = (ts_block[0]);
        min_delta[1] = (value_delta_min);
        min_delta[2] = (value_delta_max-value_delta_min);

        for (int i = 0; i  < block_size; i++) {
            int epsilon_v = ts_block[i+1] - value_delta_min - ts_block[i];
            ts_block_delta[i] = epsilon_v;
        }
        for(int i = block_size;i<block_size+supple_length;i++){
            ts_block_delta[i] = 0;
        }
        return ts_block_delta;
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

//    public static int[] getAbsDeltaTsBlock(
//            int[] ts_block,
//            int i,
//            int block_size,
//            int remaining,
//            int[] min_delta) {
//        int[] ts_block_delta = new int[block_size-1];
//
//
//        int value_delta_min = Integer.MAX_VALUE;
//        int value_delta_max = Integer.MIN_VALUE;
//        for (int j = i*block_size+1; j < i*block_size+remaining; j++) {
//
//            int epsilon_v = ts_block[j] - ts_block[j - 1];
//
//            if (epsilon_v < value_delta_min) {
//                value_delta_min = epsilon_v;
//            }
//            if (epsilon_v > value_delta_max) {
//                value_delta_max = epsilon_v;
//            }
//
//        }
//        min_delta[0] = (ts_block[i*block_size]);
//        min_delta[1] = (value_delta_min);
//        min_delta[2] = (value_delta_max-value_delta_min);
//
//        int base = i*block_size+1;
//        int end = i*block_size + remaining;
//        for (int j = base; j < end; j++) {
//            int epsilon_v = ts_block[j] - value_delta_min - ts_block[j - 1];
//            ts_block_delta[j-base] =epsilon_v;
//        }
//        return ts_block_delta;
//    }


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

    private static int BOSEncodeBits(int[] ts_block_delta,
                                     int final_k_start_value,
                                     int final_k_end_value,
                                     int max_delta_value,
                                     int[] min_delta,
                                     int encode_pos,
                                     byte[] cur_byte) {
        int block_size = ts_block_delta.length;

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
            if ( cur_value<= final_k_start_value) {
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


            } else if (cur_value >= final_k_end_value) {
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
                final_normal.add(cur_value - final_k_start_value-1);
                index_bitmap_outlier <<= 1;
                cur_index_bitmap_outlier_bits += 1;
            }
            if (cur_index_bitmap_outlier_bits % 8 == 0) {
                bitmap_outlier.add(index_bitmap_outlier);
                index_bitmap_outlier = 0;
            }
        }
        if (cur_index_bitmap_outlier_bits % 8 != 0) {

            index_bitmap_outlier <<= (8 - cur_index_bitmap_outlier_bits % 8);

            index_bitmap_outlier &= 0xFF;
            bitmap_outlier.add(index_bitmap_outlier);
        }

        int final_alpha = ((k1 + k2) * getBitWith(block_size-1)) <= (block_size + k1 + k2) ? 1 : 0;


        int k_byte = (k1 << 1);
        k_byte += final_alpha;
        k_byte += (k2 << 16);


        int2Bytes(k_byte,encode_pos,cur_byte);
        encode_pos += 4;

        int2Bytes(min_delta[0],encode_pos,cur_byte);
        encode_pos += 4;
        int2Bytes(min_delta[1],encode_pos,cur_byte);
        encode_pos += 4;
        int2Bytes(final_k_start_value,encode_pos,cur_byte);
        encode_pos += 4;
        int bit_width_final = getBitWith(final_k_end_value - final_k_start_value-2);
        intByte2Bytes(bit_width_final,encode_pos,cur_byte);
        encode_pos += 1;
        int left_bit_width = getBitWith(final_k_start_value);//final_left_max
        int right_bit_width = getBitWith(max_delta_value - final_k_end_value);//final_right_min
        intByte2Bytes(left_bit_width,encode_pos,cur_byte);
        encode_pos += 1;
        intByte2Bytes(right_bit_width,encode_pos,cur_byte);
        encode_pos += 1;
        if (final_alpha == 0) { // 0

            for (int i : bitmap_outlier) {

                intByte2Bytes(i,encode_pos,cur_byte);
                encode_pos += 1;
            }
        } else {
            encode_pos = encodeOutlier2Bytes(final_left_outlier_index, getBitWith(block_size-1),encode_pos,cur_byte);
            encode_pos = encodeOutlier2Bytes(final_right_outlier_index, getBitWith(block_size-1),encode_pos,cur_byte);
        }
        encode_pos = encodeOutlier2Bytes(final_normal, bit_width_final,encode_pos,cur_byte);
        if (k1 != 0)
            encode_pos = encodeOutlier2Bytes(final_left_outlier, left_bit_width,encode_pos,cur_byte);
        if (k2 != 0)
            encode_pos = encodeOutlier2Bytes(final_right_outlier, right_bit_width,encode_pos,cur_byte);
        return encode_pos;

    }

    public static class Group {
        private int[] number;
        private int count;

        public Group(int[] number, int count) {
            this.number = number;
            this.count = count;
        }

        public int[] getNumber() {
            return number;
        }


        public int getCount() {
            return count;
        }

        public void addNumber(int number) {
            this.number[this.count] = number;
            this.count++;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public void incrementCount() {
            count++;
        }

        @Override
        public String toString() {
            return "Number: " + number + ", Count: " + count;
        }
    }

    public static int minNumberIndex(int alpha, int beta, int gamma){
        if(alpha<beta && beta < gamma){
            return 1; // prop 4.2
        } else if (alpha>beta && beta > gamma) {
            return 2;// prop 4.3
        } else if (beta<alpha && beta <gamma) {
            return 3;// prop 4.4
        }else{
            return 0; // alpha=beta=gamma
        }
    }
    private static int BOSBlockEncoder(int[] ts_block, int block_i, int block_size, int remaining, int encode_pos, byte[] cur_byte) {

        int[] min_delta = new int[3];
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, block_size, remaining, min_delta);

        block_size = remaining-1;
        int max_delta_value = min_delta[2];


        int max_bit_width = getBitWith(max_delta_value)+1;
        int[] alpha_count_list = new int[max_bit_width];//count(xmin) count(xmin + 2) count(xmin + 4)...
//        int[] alpha_box_count_list = new int[max_bit_width];// count(xmin, xmin + 2), count(xmin + 2, xmin + 4)...
        int[] gamma_count_list = new int[max_bit_width];
//        int[] gamma_box_count_list = new int[max_bit_width];
        Group[] groupL = new Group[max_bit_width];
        Group[] groupU = new Group[max_bit_width];
        for (int i = 0; i < max_bit_width; i++) {
            int[] numbers = new int[block_size];
            int count = 0;
            groupL[i] = new Group(numbers, count);
            int[] numbersU = new int[block_size];
            groupU[i] = new Group(numbersU, count);
        }
        for(int value:ts_block_delta){
            int alpha_i = getBitWith(value); // 0 1 2 3 4
            if (value == 0){
                alpha_count_list[0]++;
            }
            else if (value == pow(2,alpha_i-1)){ // x_min+2^{alpha_i-1}
                alpha_count_list[alpha_i]++;
            }else {
                groupL[alpha_i].addNumber(value); // (x_min+2^{alpha_i-1},x_min+2^{alpha_i})
//                alpha_box_count_list[alpha_i]++;
            }
            int gamma_i = getBitWith(max_delta_value-value);
            if (value == max_delta_value){
                gamma_count_list[0]++;
            }
            else if (max_delta_value - value == pow(2,gamma_i-1)){
                gamma_count_list[gamma_i]++;
            }else {
                groupU[gamma_i].addNumber(value);// (x_max-2^{gamma_i},x_max-2^{gamma_i-1})
//                gamma_box_count_list[gamma_i]++;
            }
        }

        int final_k_start_value = -1;//getUniqueValue(sorted_value_list[0], left_shift);
        int final_k_end_value = max_delta_value+1;

        int min_bits = 0;
        min_bits += (getBitWith(final_k_end_value - final_k_start_value -2) * (block_size));

        int alpha_size = getBitWith(max_delta_value);
        int cur_k1_close = alpha_count_list[0];
        cur_k1_close += alpha_count_list[1];
        cur_k1_close += groupL[0].count ;//alpha_box_count_list[0];
        for (int alpha = 0; alpha + 1 <= alpha_size; alpha++) { //start_value_size
            //C1 k1 close k2 close
            int k_start_value_close = (int) pow(2,alpha);//close
//            System.out.println(k_start_value_close);
            Group cur_group_alpha = groupL[alpha]; // (x_min+2^{alpha_i-1},x_min+2^{alpha_i})
            cur_k1_close += alpha_count_list[alpha+1]; // x_min+2^{alpha_i}
            cur_k1_close += cur_group_alpha.count;//alpha_box_count_list[alpha];//(x_min+2^{alpha_i-1},x_min+2^{alpha_i})
            int cur_bits;
            int alpha_2_pow = (int)pow(2,alpha);
            int cur_k2_close = gamma_count_list[0];
            cur_k2_close += gamma_count_list[1];
            cur_k2_close += groupU[0].count ;//gamma_box_count_list[0];

            for (int gamma = 0; gamma <= alpha_size; gamma++) {
                int k_end_value_close = max_delta_value - (int) pow(2, gamma);
                if (max_delta_value - (int) pow(2, gamma) < 0){
                    k_end_value_close = 0;
                }
                Group cur_group_gamma = groupU[gamma];
                if (gamma + 1 <= alpha_size) {
                    cur_k2_close += gamma_count_list[gamma + 1]; // x_max-2^{gamma_i}
                }
                cur_k2_close += cur_group_gamma.count;//gamma_box_count_list[gamma]; //(x_max-2^{gamma_i},x_max-2^{gamma_i-1})
                int cur_k1_open = cur_k1_close - alpha_count_list[alpha + 1];
                int cur_k2_open = cur_k2_close;
                if (gamma + 1 <= alpha_size) {
                    cur_k2_open = cur_k2_close - gamma_count_list[gamma + 1];
                }
                int k_start_value_open = k_start_value_close - 1;
                int k_end_value_open = k_end_value_close + 1;
                if (k_end_value_close > k_start_value_close) {
                    cur_bits = 0;
                    int flag_C = 0;

                    // prop 4.5
                    cur_bits += Math.min((cur_k1_close + cur_k2_close) * getBitWith(block_size - 1), block_size + cur_k1_close + cur_k2_close);
                    if (cur_k1_close != 0)
                        cur_bits += cur_k1_close * (alpha + 1);
                    if (cur_k1_close + cur_k2_close != block_size)
                        cur_bits += (block_size - cur_k1_close - cur_k2_close) * getBitWith(k_end_value_close - k_start_value_close - 2);
                    if (cur_k2_close != 0)
                        cur_bits += cur_k2_close * (gamma + 1);//min_upper_outlier

                    if (cur_bits < min_bits) {
                        min_bits = cur_bits;
                        flag_C = 1;
                    }

                    // prop 4.2
                    //C2 k1open k2 open
                    cur_bits = 0;
                    cur_bits += Math.min((cur_k1_open + cur_k2_open) * getBitWith(block_size - 1), block_size + cur_k1_open + cur_k2_open);
                    if (cur_k1_open != 0)
                        cur_bits += cur_k1_open * alpha;
                    if (cur_k1_open + cur_k2_open != block_size)
                        cur_bits += (block_size - cur_k1_open - cur_k2_open) * getBitWith(k_end_value_open - k_start_value_open - 2);
                    if (cur_k2_open != 0)
                        cur_bits += cur_k2_open * gamma;


                    if (cur_bits < min_bits) {
                        min_bits = cur_bits;
                        flag_C = 2;
                    }

                    // prop 4.3
                    //C3 k1 open k2 close
                    cur_bits = 0;
                    cur_bits += Math.min((cur_k1_open + cur_k2_close) * getBitWith(block_size - 1), block_size + cur_k1_open + cur_k2_close);
                    if (cur_k1_open != 0)
                        cur_bits += cur_k1_open * alpha;
                    if (cur_k1_open + cur_k2_close != block_size)
                        cur_bits += (block_size - cur_k1_open - cur_k2_close) * getBitWith(k_end_value_close - k_start_value_open - 2);
                    if (cur_k2_close != 0)
                        cur_bits += cur_k2_close * (gamma + 1);//min_upper_outlier

                    if (cur_bits < min_bits) {
                        min_bits = cur_bits;
                        flag_C = 3;
                    }

                    // prop 4.4
                    //C4 k1 close k2 open
                    cur_bits = 0;
                    cur_bits += Math.min((cur_k1_close + cur_k2_open) * getBitWith(block_size - 1), block_size + cur_k1_close + cur_k2_open);
                    if (cur_k1_close != 0)
                        cur_bits += cur_k1_close * (alpha + 1);
                    if (cur_k1_close + cur_k2_open != block_size)
                        cur_bits += (block_size - cur_k1_close - cur_k2_open) * getBitWith(k_end_value_open - k_start_value_close - 2);
                    if (cur_k2_open != 0)
                        cur_bits += cur_k2_open * gamma;//min_upper_outlier

                    if (cur_bits < min_bits) {
                        min_bits = cur_bits;
                        flag_C = 4;
                    }
                    if (flag_C == 1) {
                        final_k_start_value = k_start_value_close;
                        final_k_end_value = k_end_value_close;
                    } else if (flag_C == 2) {
                        final_k_start_value = k_start_value_open;
                        final_k_end_value = k_end_value_open;
                    } else if (flag_C == 3) {
                        final_k_start_value = k_start_value_open;
                        final_k_end_value = k_end_value_close;
                    } else if (flag_C == 4) {
                        final_k_start_value = k_start_value_close;
                        final_k_end_value = k_end_value_open;
                    }
                }

                int gamma_2_pow = (int)pow(2,gamma);

                int gap_alpha = alpha_2_pow/2;
                int alpha_value_count_list_start = gap_alpha;

                int[] alpha_value_count_list = new int[gap_alpha];
                int alpha_value_count = cur_group_alpha.count;
                int[] number_alpha = cur_group_alpha.number;
                for(int i=0;i<alpha_value_count;i++){
                    int value = number_alpha[i];
                    alpha_value_count_list[value-alpha_value_count_list_start] ++;
                }

                int gap_gamma = gamma_2_pow/2;
                int gamma_value_count_list_end = max_delta_value - gap_gamma;
                if (gamma_value_count_list_end <= alpha_value_count_list_start){
                    break;
                }
                int[] gamma_value_count_list = new int[gap_gamma];
                int gamma_value_count = cur_group_gamma.count;
                int[] number_gamma = cur_group_gamma.number;

                int[] a_list = {getBitWith(block_size-1),1};
                int[] b_list = {0,block_size};
                for(int i=0;i<gamma_value_count;i++){
                    int value = number_gamma[i];
                    gamma_value_count_list[gamma_value_count_list_end-value] ++;
                }

                for(int a_i= 0; a_i<2; a_i++){
                    int a = a_list[a_i];
                    int b = b_list[a_i];
                    int cur_k1_x_l =  cur_k1_open - cur_group_alpha.count;
                    for (int x_l_i = 0; x_l_i < gap_alpha; x_l_i++) {
                        int cur_count_alpha = alpha_value_count_list[x_l_i];
                        if (cur_count_alpha == 0)
                            continue;
                        cur_k1_x_l += cur_count_alpha;// gamma_box_count_list[gamma];
                        int cur_k1_x_u = cur_k2_open -cur_group_gamma.count;
                        for (int x_u_i = 0; x_u_i < gap_gamma; x_u_i++) {
                            if (gamma_value_count_list_end - x_u_i <= alpha_value_count_list_start + x_l_i){
                                break;
                            }
                            int cur_count_gamma = gamma_value_count_list[x_u_i];
                            if (cur_count_gamma == 0)
                                continue;
                            cur_k1_x_u += cur_count_gamma;

                            cur_bits = (cur_k1_x_l + cur_k1_x_u) * a + b;
                            if (cur_k1_x_l != 0)
                                cur_bits += cur_k1_x_l * alpha;
                            if (cur_k1_x_l + cur_k1_x_u != block_size)
                                cur_bits += (block_size - cur_k1_x_l - cur_k1_x_u) *
                                        getBitWith(gamma_value_count_list_end - alpha_value_count_list_start - x_l_i - x_u_i - 2);
                            if (cur_k1_x_u != 0)
                                cur_bits += cur_k1_x_u * gamma;//min_upper_outlier
                            if(alpha_value_count_list_start == 1){
                                System.out.println(gamma_value_count_list_end - x_u_i);
                                System.out.println(alpha);
                                System.out.println("cur_bits:"+cur_bits);
                            }
                            if (cur_bits < min_bits) {
                                min_bits = cur_bits;
                                final_k_start_value = alpha_value_count_list_start + x_l_i;
                                final_k_end_value = gamma_value_count_list_end - x_u_i; //need to check again
                            }
                        }
                    }
                }
            }

        }
        Group cur_group_alpha = groupL[alpha_size]; // (x_min+2^{alpha_i-1},x_min+2^{alpha_i})
        int k_start_value_close = max_delta_value;
        cur_k1_close += cur_group_alpha.count;//alpha_box_count_list[alpha];//(x_min+2^{alpha_i-1},x_min+2^{alpha_i})
        int cur_bits;
        int alpha_2_pow = (int)pow(2,alpha_size);
        int cur_k2_close = gamma_count_list[0];
        cur_k2_close += gamma_count_list[1];
        cur_k2_close += groupU[0].count ;//gamma_box_count_list[0];
        for (int gamma = 0; gamma <= alpha_size; gamma++) {
            int k_end_value_close = max_delta_value - (int) pow(2, gamma);
            Group cur_group_gamma = groupU[gamma];
            if (gamma + 1 <= alpha_size) {
                cur_k2_close += gamma_count_list[gamma + 1]; // x_max-2^{gamma_i}
            }
            cur_k2_close += cur_group_gamma.count;//gamma_box_count_list[gamma]; //(x_max-2^{gamma_i},x_max-2^{gamma_i-1})
            int cur_k1_open = cur_k1_close;
            int cur_k2_open = cur_k2_close;
            if (gamma + 1 <= alpha_size) {
                cur_k2_open = cur_k2_close - gamma_count_list[gamma + 1];
            }
            int k_start_value_open = k_start_value_close - 1;
            int k_end_value_open = k_end_value_close + 1;
            int gamma_2_pow = (int) pow(2, gamma);

            int gap_alpha = alpha_2_pow / 2;
            int alpha_value_count_list_start = gap_alpha;
            int gap_gamma = gamma_2_pow / 2;
            int gamma_value_count_list_end = max_delta_value - gap_gamma;
            if (gamma_value_count_list_end <= alpha_value_count_list_start) {
                break;
            }
            int[] alpha_value_count_list = new int[gap_alpha];
            int alpha_value_count = cur_group_alpha.count;
            int[] number_alpha = cur_group_alpha.number;
            for (int i = 0; i < alpha_value_count; i++) {
                int value = number_alpha[i];
                alpha_value_count_list[value - alpha_value_count_list_start]++;
            }

            int[] gamma_value_count_list = new int[gap_gamma];
            int gamma_value_count = cur_group_gamma.count;
            int[] number_gamma = cur_group_gamma.number;

            int[] a_list = {getBitWith(block_size - 1), 1};
            int[] b_list = {0, block_size};
            for (int i = 0; i < gamma_value_count; i++) {
                int value = number_gamma[i];
                gamma_value_count_list[gamma_value_count_list_end - value]++;
            }

            for (int a_i = 0; a_i < 2; a_i++) {
                int a = a_list[a_i];
                int b = b_list[a_i];
                int cur_k1_x_l = cur_k1_open - cur_group_alpha.count;
                for (int x_l_i = 0; x_l_i < gap_alpha; x_l_i++) {
                    int cur_count_alpha = alpha_value_count_list[x_l_i];
                        if (cur_count_alpha == 0)
                            continue;
                    cur_k1_x_l += cur_count_alpha;// gamma_box_count_list[gamma];
                    int cur_k1_x_u = cur_k2_open - cur_group_gamma.count;
                    for (int x_u_i = 0; x_u_i < gap_gamma; x_u_i++) {
                        if (gamma_value_count_list_end - x_u_i <= alpha_value_count_list_start + x_l_i) {
                            break;
                        }
                        int cur_count_gamma = gamma_value_count_list[x_u_i];
                            if (cur_count_gamma == 0)
                                continue;
                        cur_k1_x_u += cur_count_gamma;

                        cur_bits = (cur_k1_x_l + cur_k1_x_u) * a + b;
                        if (cur_k1_x_l != 0)
                            cur_bits += cur_k1_x_l * alpha_size;
                        if (cur_k1_x_l + cur_k1_x_u != block_size)
                            cur_bits += (block_size - cur_k1_x_l - cur_k1_x_u) *
                                    getBitWith(gamma_value_count_list_end - alpha_value_count_list_start - x_l_i - x_u_i - 2);
                        if (cur_k1_x_u != 0)
                            cur_bits += cur_k1_x_u * gamma;//min_upper_outlier
//                            if(alpha_value_count_list_start + x_l_i== 143){
//                                System.out.println(gamma_value_count_list_end - x_u_i);
//                                System.out.println(alpha);
//                                System.out.println("cur_bits:"+cur_bits);
//                            }
                        if (cur_bits < min_bits) {
                            min_bits = cur_bits;
                            final_k_start_value = alpha_value_count_list_start + x_l_i;
                            final_k_end_value = gamma_value_count_list_end - x_u_i; //need to check again
                        }
                    }
                }
            }
        }
        // 48159 64437 15532 1 6 5868
        encode_pos = BOSEncodeBits(ts_block_delta,  final_k_start_value, final_k_end_value, max_delta_value,
                min_delta, encode_pos , cur_byte);

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


        int k_byte = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        int k1_byte = (int) (k_byte % pow(2, 16));
        int k1 = k1_byte / 2;
        int final_alpha = k1_byte % 2;

        int k2 = (int) (k_byte / pow(2, 16));

        int value0 = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;
        value_list[value_pos_arr[0]] =value0;
        value_pos_arr[0] ++;

        int min_delta = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int final_k_start_value = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        int bit_width_final = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        int left_bit_width = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;
        int right_bit_width = bytes2Integer(encoded, decode_pos, 1);
        decode_pos += 1;

        ArrayList<Integer> final_left_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_right_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_left_outlier = new ArrayList<>();
        ArrayList<Integer> final_right_outlier = new ArrayList<>();
        ArrayList<Integer> final_normal= new ArrayList<>();;
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();


        if (final_alpha == 0) {
            int bitmap_bytes = (int) Math.ceil((double) (block_size + k1 + k2) / (double) 8);
            for (int i = 0; i < bitmap_bytes; i++) {
                bitmap_outlier.add(bytes2Integer(encoded, decode_pos, 1));
                decode_pos += 1;
            }
            int bitmap_outlier_i = 0;
            int remaining_bits = 8;
            int tmp = bitmap_outlier.get(bitmap_outlier_i);
            bitmap_outlier_i++;
            int i = 0;
            while (i < block_size ) {
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
            ArrayList<Integer> decode_pos_result_left = new ArrayList<>();
            final_left_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(block_size-1), k1, decode_pos_result_left);
            decode_pos = (decode_pos_result_left.get(0));
            ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
            final_right_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(block_size-1), k2, decode_pos_result_right);
            decode_pos = (decode_pos_result_right.get(0));
        }


        ArrayList<Integer> decode_pos_normal = new ArrayList<>();
//        if(k1+k2!=block_size)
        final_normal = decodeOutlier2Bytes(encoded, decode_pos, bit_width_final, block_size - k1 - k2, decode_pos_normal);

        decode_pos = decode_pos_normal.get(0);
        if (k1 != 0) {
            ArrayList<Integer> decode_pos_result_left = new ArrayList<>();
            final_left_outlier = decodeOutlier2Bytes(encoded, decode_pos, left_bit_width, k1, decode_pos_result_left);
            decode_pos = decode_pos_result_left.get(0);
        }
        if (k2 != 0) {
            ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
            final_right_outlier = decodeOutlier2Bytes(encoded, decode_pos, right_bit_width, k2, decode_pos_result_right);
            decode_pos = decode_pos_result_right.get(0);
        }
        int left_outlier_i = 0;
        int right_outlier_i = 0;
        int normal_i = 0;
        int pre_v = value0;
        int final_k_end_value = (int) (final_k_start_value + pow(2, bit_width_final));


        for (int i = 0; i < block_size; i++) {
            int current_delta;
            if (left_outlier_i >= k1) {
                if (right_outlier_i >= k2) {
                    current_delta = min_delta + final_normal.get(normal_i) + final_k_start_value;
                    normal_i++;
                } else if (i == final_right_outlier_index.get(right_outlier_i)) {
                    current_delta = min_delta + final_right_outlier.get(right_outlier_i) + final_k_end_value;
                    right_outlier_i++;
                } else {
                    current_delta = min_delta + final_normal.get(normal_i) + final_k_start_value;
                    normal_i++;
                }
            } else if (i == final_left_outlier_index.get(left_outlier_i)) {
                current_delta = min_delta + final_left_outlier.get(left_outlier_i);
                left_outlier_i++;
            } else {

                if (right_outlier_i >= k2) {
                    current_delta = min_delta + final_normal.get(normal_i) + final_k_start_value;
                    normal_i++;
                } else if (i == final_right_outlier_index.get(right_outlier_i)) {
                    current_delta = min_delta + final_right_outlier.get(right_outlier_i) + final_k_end_value;
                    right_outlier_i++;
                } else {
                    current_delta = min_delta + final_normal.get(normal_i) + final_k_start_value;
                    normal_i++;
                }
            }

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
    public void PruneBOSTest() throws IOException {
//        String parent_dir = "/Users/xiaojinzhao/Desktop/encoding-outlier/"; // your data path
        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "vldb/compression_ratio/pruning_bos";
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

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
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

        int repeatTime2 = 1;
        for (int file_i = 4; file_i < 5; file_i++) {
//
//        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

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
//                f= tempList[3];
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();


                loader.readHeaders();
                while (loader.readRecord()) {
//                        String value = loader.getValues()[index];
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
//                        data.add(Integer.valueOf(value));
                }
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


                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length =  BOSEncoder(data2_arr, dataset_block_size.get(file_i), encoded_result);
                }

                long e = System.nanoTime();
                encodeTime += ((e - s) / repeatTime2);
                compressed_size += length;
                double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++)
                    BOSDecoder(encoded_result);
                e = System.nanoTime();
                decodeTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "TS_2DIFF+BOS-P",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
//break;
            }
            writer.close();

        }
    }


}
