package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.transform.*;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class ComplexWithIndex {
    Complex value;
    int index;

    public ComplexWithIndex(Complex value, int index) {
        this.value = value;
        this.index = index;
    }
}

public class FFT_IFFT {

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

    public static int EncodeBits(int num,
                                 int bit_width,
                                 int encode_pos,
                                 byte[] cur_byte,
                                 int[] bit_index_list){
        // 找到要插入的位的索引
        int bit_index = bit_index_list[0] ;//cur_byte[encode_pos + 1];

        // 计算数值的起始位位置
        int remaining_bits = bit_width;

        while (remaining_bits > 0) {
            // 计算在当前字节中可以使用的位数
            int available_bits = bit_index;
            int bits_to_write = Math.min(available_bits, remaining_bits);

            // 更新 bit_index
            bit_index = available_bits - bits_to_write;

            // 计算要写入的位的掩码和数值
            int mask = (1 << bits_to_write) - 1;
            int bits = (num >> (remaining_bits - bits_to_write)) & mask;

            // 写入到当前位置
            cur_byte[encode_pos] &= (byte) ~(mask << bit_index); // 清除对应位置的位
            cur_byte[encode_pos] |= (byte) (bits << bit_index);

            // 更新位宽和数值
            remaining_bits -= bits_to_write;
            if (bit_index == 0) {
                bit_index = 8;
                encode_pos++;
            }
        }
        bit_index_list[0] = bit_index;
        return encode_pos;
    }

    private static void long2intBytes(long integer, int encode_pos , byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos+1] = (byte) (integer >> 16);
        cur_byte[encode_pos+2] = (byte) (integer >> 8);
        cur_byte[encode_pos+3] = (byte) (integer);
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
    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width,int encode_pos,  byte[] encoded_result) {
        int block_num = (numbers.size()-start) / 8;
        for(int i=0;i<block_num;i++){
            pack8Values( numbers, start+i*8, bit_width,encode_pos, encoded_result);
            encode_pos +=bit_width;
        }
        return encode_pos;
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

    private static int BOSEncodeBitsImprove(int[] ts_block_delta,
                                            int final_k_start_value,
                                            int final_x_l_plus,
                                            int final_k_end_value,
                                            int final_x_u_minus,
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

        int bit_width_final = getBitWith(final_x_u_minus - final_x_l_plus);
        intByte2Bytes(bit_width_final,encode_pos,cur_byte);
        encode_pos += 1;
        int[] bit_index_list = new int[1];
        bit_index_list[0] = 8;

        if(final_k_start_value<0 && final_k_end_value > max_delta_value){
            for (int cur_value : ts_block_delta) {
                encode_pos = EncodeBits(cur_value, bit_width_final, encode_pos, cur_byte, bit_index_list);
            }
            if(bit_index_list[0] != 8){
                encode_pos ++;
            }
            return encode_pos;
        }
        int left_bit_width = getBitWith(final_k_start_value);//final_left_max
        int right_bit_width = getBitWith(max_delta_value - final_k_end_value);//final_right_min
        int2Bytes(final_x_l_plus,encode_pos,cur_byte);
        encode_pos += 4;
        int2Bytes(final_k_end_value,encode_pos,cur_byte);
        encode_pos += 4;

//        bit_width_final = getBitWith(final_x_u_minus - final_x_l_plus);
//        intByte2Bytes(bit_width_final,encode_pos,cur_byte);
//        encode_pos += 1;
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
//        cur_byte[encode_pos+1] = 8;
//        bit_index_list[0] = 8;
        for (int cur_value : ts_block_delta) {
            if (cur_value <= final_k_start_value) {
                encode_pos = EncodeBits(cur_value, left_bit_width, encode_pos, cur_byte,bit_index_list);
            } else if (cur_value >= final_k_end_value) {
                encode_pos = EncodeBits(cur_value - final_k_end_value, right_bit_width, encode_pos, cur_byte,bit_index_list);
            } else {
                encode_pos = EncodeBits(cur_value - final_x_l_plus, bit_width_final, encode_pos, cur_byte,bit_index_list);
            }
        }
        if(bit_index_list[0] != 8){
            encode_pos ++;
        }
        return encode_pos;
    }
    static int blockSize = 256;// 使用的blocksize，只能使用2的幂次
    static int n = 3;// 选取前几个分量，需要保证n < blockSize
    public static void getTopNMaxAbsoluteValues(double[] array, int n, double[] values, int[] indices) {
        if (array == null || array.length < n) {
            throw new IllegalArgumentException("Array must contain at least " + n + " elements.");
        }

        List<ValueWithIndex> list = new ArrayList<>();
        for (int i = 0; i < array.length; i++) {
            list.add(new ValueWithIndex(array[i], i));
        }

        list.sort((o1, o2) -> Double.compare(Math.abs(o2.value), Math.abs(o1.value)));

        for (int i = 0; i < n; i++) {
            values[i] = list.get(i).value;
            indices[i] = list.get(i).index;
        }
    }

    public static void getTopNMaxAbsoluteValues(Complex[] array, int n, Complex[] values, int[] indices) {
        if (array == null || array.length < n) {
            throw new IllegalArgumentException("Array must contain at least " + n + " elements.");
        }

        List<ComplexWithIndex> list = new ArrayList<>();
        for (int i = 0; i < array.length; i++) {
            list.add(new ComplexWithIndex(array[i], i));
        }

        list.sort((o1, o2) -> Double.compare(o2.value.abs(), o1.value.abs()));

        for (int i = 0; i < n; i++) {
            values[i] = list.get(i).value;
            indices[i] = list.get(i).index;
        }
    }
    private static final FastCosineTransformer dctTransformer = new FastCosineTransformer(DctNormalization.ORTHOGONAL_DCT_I);
    private static final FastFourierTransformer fft = new FastFourierTransformer(DftNormalization.STANDARD);
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

    private static int BOSBlockEncoderImprove(int[] ts_block,int block_i,int block_size, int remaining ,int encode_pos , byte[] cur_byte) {
        int[] min_delta = new int[3];
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, block_size, remaining, min_delta);
        block_size = remaining-1;
        int max_delta_value = min_delta[2];
        int[] value_list = new int[block_size];
        int unique_value_count = 0;
        int[] value_count_list = new int[max_delta_value+1];
        for(int value:ts_block_delta){
            if(value_count_list[value]==0){
                value_count_list[value] = 1;
                value_list[unique_value_count] = value;
                unique_value_count ++;
            }else{
                value_count_list[value] ++;
            }
        }

        int left_shift = getBitWith(block_size);
        int mask =  (1 << left_shift) - 1;
        long[] sorted_value_list = new long[unique_value_count];
        int count = 0;

        for(int i=0;i<unique_value_count;i++){
            int value = value_list[i];
            sorted_value_list[i] = (((long) value) << left_shift) + value_count_list[value];
        }
        Arrays.sort(sorted_value_list);

        for(int i=0;i<unique_value_count;i++){
            count += getCount(sorted_value_list[i], mask);
            sorted_value_list[i] = (((long)getUniqueValue(sorted_value_list[i], left_shift) ) << left_shift) + count;//new_value_list[i]
        }


        int final_k_start_value = -1; // x_l_minus
        int final_x_l_plus = 0; // x_l_plus
        int final_k_end_value = max_delta_value+1; // x_u_plus
        int final_x_u_minus = max_delta_value; // x_u_minus

        int min_bits = 0;
        min_bits += (getBitWith(final_k_end_value - final_k_start_value - 2 ) * (block_size));

        int cur_k1 = 0;

        int x_l_plus_value = 0; // x_l_plus
        int x_u_minus_value = max_delta_value; // x_u_plus

        for (int end_value_i = 1; end_value_i < unique_value_count; end_value_i++) {

            x_u_minus_value = getUniqueValue(sorted_value_list[end_value_i-1], left_shift);
            int x_u_plus_value = getUniqueValue(sorted_value_list[end_value_i], left_shift);
            int cur_bits = 0;
            int cur_k2 = block_size - getCount(sorted_value_list[end_value_i-1],mask);
            cur_bits += Math.min((cur_k2 + cur_k1) * getBitWith(block_size-1), block_size + cur_k2 + cur_k1);
            if (cur_k1 + cur_k2 != block_size)
                cur_bits += (block_size - cur_k2) * getBitWith(x_u_minus_value - x_l_plus_value); // cur_k1 = 0
            if (cur_k2 != 0)
                cur_bits += cur_k2 * getBitWith(max_delta_value - x_u_plus_value);


            if (cur_bits < min_bits) {
                min_bits = cur_bits;
                final_x_u_minus = x_u_minus_value;
                final_k_end_value = x_u_plus_value;
            }
        }

        int k_start_value = -1;
        int gamma_max = getBitWith(max_delta_value);
        int[] gamma_count_list = new int[gamma_max+1];
        int[] x_u_minus_value_list = new int[gamma_max+1];
        int[] x_u_plus_value_list = new int[gamma_max+1];
        int end_i = unique_value_count - 1;
        for(int gamma = 0; gamma <= gamma_max; gamma++) {
            int x_u_plus_pow_beta = max_delta_value - (1<<gamma) + 1;
            for (; end_i > 0; end_i--) {
                x_u_minus_value = getUniqueValue(sorted_value_list[end_i - 1], left_shift);
                int x_u_plus_value = getUniqueValue(sorted_value_list[end_i], left_shift);
                if (x_u_minus_value < x_u_plus_pow_beta && x_u_plus_value >= x_u_plus_pow_beta){
                    gamma_count_list[gamma] = getCount(sorted_value_list[end_i-1],mask);
                    x_u_minus_value_list[gamma] = x_u_minus_value;
                    x_u_plus_value_list[gamma] = x_u_plus_value;
                } else if (x_u_minus_value < x_u_plus_pow_beta) {
                    break;
                }
            }
        }
        for(int gamma = 1; gamma < gamma_max; gamma++) {
            if(gamma_count_list[gamma]==0){
                gamma_count_list[gamma] = gamma_count_list[gamma-1];
                x_u_minus_value_list[gamma] = x_u_minus_value_list[gamma-1];
                x_u_plus_value_list[gamma] = x_u_plus_value_list[gamma-1];
            }
        }

        for (int start_value_i = 0; start_value_i < unique_value_count-1; start_value_i++) {
            long k_start_valueL = sorted_value_list[start_value_i];
            k_start_value =  getUniqueValue(k_start_valueL, left_shift) ;
            x_l_plus_value =  getUniqueValue(sorted_value_list[start_value_i+1], left_shift) ;


            cur_k1 = getCount(k_start_valueL,mask);

            int k_end_value;
            int cur_bits;
            int cur_k2;
            k_end_value = max_delta_value + 1;

            cur_bits = 0;
            cur_k2 = 0;
            cur_bits += Math.min((cur_k2 + cur_k1) * getBitWith(block_size-1), block_size + cur_k2 + cur_k1);
            cur_bits += cur_k1 * getBitWith(k_start_value);
            if (cur_k1 + cur_k2 != block_size)
                cur_bits += (block_size - cur_k1) * getBitWith(k_end_value- x_l_plus_value); //cur_k2 =0

            if (cur_bits < min_bits) {
                min_bits = cur_bits;
                final_k_start_value = k_start_value;
                final_x_l_plus = x_l_plus_value;
                final_k_end_value = k_end_value;
                final_x_u_minus = max_delta_value;
            }

            int beta_max = getBitWith(max_delta_value - x_l_plus_value);

            int lower_outlier_cost = cur_k1 * getBitWith(k_start_value);



            for(int gamma = 0; gamma < beta_max; gamma++){
                x_u_minus_value = x_u_minus_value_list[gamma];
                k_end_value =  x_u_plus_value_list[gamma];
                cur_bits = 0;
                cur_k2 = block_size - gamma_count_list[gamma];

                cur_bits += Math.min((cur_k1 + cur_k2) * getBitWith(block_size-1), block_size + cur_k1 + cur_k2);
                cur_bits += lower_outlier_cost;
                if (cur_k1 + cur_k2 != block_size)
                    cur_bits += (block_size - cur_k1 - cur_k2) * getBitWith(x_u_minus_value - x_l_plus_value);
                if (cur_k2 != 0)
                    cur_bits += cur_k2 * getBitWith(max_delta_value - k_end_value);


                if (cur_bits < min_bits) {
                    min_bits = cur_bits;
                    final_k_start_value = k_start_value;
                    final_x_l_plus = x_l_plus_value;
                    final_k_end_value = k_end_value;
                    final_x_u_minus = x_u_minus_value;
                }
            }
        }

        for(int beta = 0; beta < gamma_max; beta++){

            int pow_beta = 1<<beta;
            int start_value_i = 0;
            int end_value_i = start_value_i+1;

            for (; start_value_i < unique_value_count-1; start_value_i++) {
                long x_l_minusL = sorted_value_list[start_value_i];
                int x_l_minus =  getUniqueValue(x_l_minusL, left_shift) ;
                int x_l_plus =  getUniqueValue(sorted_value_list[start_value_i+1], left_shift) ;
                int x_u_plus_pow_beta = pow_beta+x_l_plus;
                if(x_u_plus_pow_beta > max_delta_value) break;



                cur_k1 = getCount(x_l_minusL,mask);
                int lower_outlier_cost = cur_k1 * getBitWith(x_l_minus);

                while ( end_value_i < unique_value_count) {
                    int x_u_minus = getUniqueValue(sorted_value_list[end_value_i-1], left_shift);
                    int x_u_plus = getUniqueValue(sorted_value_list[end_value_i], left_shift);
                    if(x_u_minus < x_u_plus_pow_beta && x_u_plus >= x_u_plus_pow_beta){
                        int cur_bits = 0;
                        int cur_k2 = block_size - getCount(sorted_value_list[end_value_i-1],mask);

                        cur_bits += Math.min((cur_k1 + cur_k2) * getBitWith(block_size-1), block_size + cur_k1 + cur_k2);
                        cur_bits += lower_outlier_cost;
                        if (cur_k1 + cur_k2 != block_size)
                            cur_bits += (block_size - cur_k1 - cur_k2) * getBitWith(x_u_minus - x_l_plus);
                        if (cur_k2 != 0)
                            cur_bits += cur_k2 * getBitWith(max_delta_value - x_u_plus);


                        if (cur_bits < min_bits) {
                            min_bits = cur_bits;
                            final_k_start_value = x_l_minus;
                            final_x_l_plus = x_l_plus;
                            final_k_end_value = x_u_plus;
                            final_x_u_minus = x_u_minus;
                        }
                        break;
                    }
                    end_value_i++;
                }
            }

        }
        encode_pos = BOSEncodeBitsImprove(ts_block_delta,  final_k_start_value, final_x_l_plus, final_k_end_value, final_x_u_minus,
                max_delta_value, min_delta, encode_pos , cur_byte);
//        System.out.println(encode_pos);
        return encode_pos;
    }

    public static ArrayList<Byte> encode(int[] timeSeries) throws IOException {
        int numberOfBlocks = (timeSeries.length + blockSize - 1) / blockSize;
        ArrayList<Byte> resList = new ArrayList<>();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(numberOfBlocks);
        byte[] byteArray = byteBuffer.array();
        for (byte temp:byteArray){
            resList.add(temp);
        }

        for (int i = 0; i < numberOfBlocks; i++) {
            if (i != numberOfBlocks - 1) {
                int start = i * blockSize;
                int end = Math.min(start + blockSize, timeSeries.length);

                int[] block = new int[end - start];
                System.arraycopy(timeSeries, start, block, 0, end - start);

                double[] d_ts = new double[block.length];
                int index = 0;
                for (int value : block) {
                    d_ts[index] = value;
                    index++;
                }
                Complex[] res = fft.transform(d_ts, TransformType.FORWARD);
                //double[] res = dctTransformer.transform(d_ts, TransformType.FORWARD);
                Complex[] values = new Complex[n];
                int[] indices = new int[n];

                try {
                    getTopNMaxAbsoluteValues(res, n, values, indices);
                    Complex[] d_f_new_res = new Complex[block.length];
                    for (int k = 0; k < block.length; k++){
                        d_f_new_res[k] = new Complex(0,0);
                    }
                    for (int k = 0; k < n; k++){
                        float r = (float)values[k].getReal();
                        float im = (float)values[k].getImaginary();
                        d_f_new_res[indices[k]] = new Complex(r,im);
                    }
                    Complex[] new_dts = fft.transform(d_f_new_res, TransformType.INVERSE);
                    int[] new_its = new int[new_dts.length];
                    for (int k = 0; k < new_dts.length; k++) {
                        new_its[k] = (int) Math.round(new_dts[k].getReal());
                    }
                    ArrayList<Integer> err = new ArrayList<>();
                    for (int k = 0; k < new_dts.length; k++) {
                        err.add(new_its[k] - block[k]);
                    }
                    // 编码new_dts进elems
                    byte[] elems = new byte[new_dts.length*4];
                    int encode_pos = 0;
                    encode_pos = bitPacking(err,0,getBitWith(blockSize),0,elems);
                    //encode_pos = BOSBlockEncoderImprove(err,0,blockSize,blockSize,encode_pos,elems);
                    // 编码new_dts进buffer
//                    for (int k = 0; k < new_dts.length; k++) {
//
//                    }
                    for (int k = 0; k < n; k++){
                        byteBuffer = ByteBuffer.allocate(4);
                        byteBuffer.putInt(indices[k]);
                        byteArray = byteBuffer.array();
                        for (byte temp:byteArray){
                            resList.add(temp);
                        }
                        byteBuffer = ByteBuffer.allocate(4);
                        byteBuffer.putFloat((float)values[k].getReal());
                        byteArray = byteBuffer.array();
                        for (byte temp:byteArray){
                            resList.add(temp);
                        }
                        byteBuffer = ByteBuffer.allocate(4);
                        byteBuffer.putFloat((float)values[k].getImaginary());
                        byteArray = byteBuffer.array();
                        for (byte temp:byteArray){
                            resList.add(temp);
                        }
                    }
                    byteBuffer = ByteBuffer.allocate(4);
                    byteBuffer.putInt(encode_pos);
                    byteArray = byteBuffer.array();
                    for (byte temp:byteArray){
                        resList.add(temp);
                    }
                    for (int l = 0; l < encode_pos; l++){
                        byte temp = elems[l];
                        resList.add(temp);
                    }
                } catch (IllegalArgumentException e) {
                    System.out.println(e.getMessage());
                }
            } else {
                int start = i * blockSize;
                int end = Math.min(start + blockSize, timeSeries.length);

                int[] block = new int[end - start];
                System.arraycopy(timeSeries, start, block, 0, end - start);
                //ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                ArrayList<Integer> block_array = new ArrayList<>();
                for (int k = 0; k < block.length; k++) {
                    block_array.add(block[k]);
                }
                // 编码new_dts进elems
                byte[] elems = new byte[block.length*4];
                int encode_pos = bitPacking(block_array,0,getBitWith(blockSize),0,elems);
//                编码block内数据
//                for (int j : block) {
//                    encoder.encode(j, buffer);
//                }
                //byte[] elems = buffer.toByteArray();
                byteBuffer = ByteBuffer.allocate(4);
                byteBuffer.putInt(encode_pos);
                byteArray = byteBuffer.array();
                for (byte temp:byteArray){
                    resList.add(temp);
                }
                for (int l = 0; l < encode_pos; l++){
                    byte temp = elems[l];
                    resList.add(temp);
                }
            }
        }
        return resList;
    }

    public static ArrayList<Byte> encode2(int[] timeSeries) throws IOException {
        int numberOfBlocks = (timeSeries.length + blockSize - 1) / blockSize;
        ArrayList<Byte> resList = new ArrayList<>();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(numberOfBlocks);
        byte[] byteArray = byteBuffer.array();
        for (byte temp:byteArray){
            resList.add(temp);
        }

        for (int i = 0; i < numberOfBlocks; i++) {
            if (i != numberOfBlocks - 1) {
                int start = i * blockSize;
                int end = Math.min(start + blockSize, timeSeries.length);

                int[] block = new int[end - start];
                System.arraycopy(timeSeries, start, block, 0, end - start);

                double[] d_ts = new double[block.length];
                int index = 0;
                for (int value : block) {
                    d_ts[index] = value;
                    index++;
                }
                Complex[] res = fft.transform(d_ts, TransformType.FORWARD);
                //double[] res = dctTransformer.transform(d_ts, TransformType.FORWARD);
                Complex[] values = new Complex[n];
                int[] indices = new int[n];

                try {
                    getTopNMaxAbsoluteValues(res, n, values, indices);
                    Complex[] d_f_new_res = new Complex[block.length];
                    for (int k = 0; k < block.length; k++){
                        d_f_new_res[k] = new Complex(0,0);
                    }
                    for (int k = 0; k < n; k++){
                        float r = (float)values[k].getReal();
                        float im = (float)values[k].getImaginary();
                        d_f_new_res[indices[k]] = new Complex(r,im);
                    }
                    Complex[] new_dts = fft.transform(d_f_new_res, TransformType.INVERSE);
                    int[] new_its = new int[new_dts.length];
                    for (int k = 0; k < new_dts.length; k++) {
                        new_its[k] = (int) Math.round(new_dts[k].getReal());
                    }
                    int[] err = new int[new_dts.length];
                    for (int k = 0; k < new_dts.length; k++) {
                        err[k] = new_its[k] - block[k];
                    }
                    // 编码new_dts进elems
                    byte[] elems = new byte[new_dts.length*4];
                    int encode_pos = 0;
                    encode_pos = BOSBlockEncoderImprove(err,0,blockSize,blockSize,encode_pos,elems);
                    // 编码new_dts进buffer
//                    for (int k = 0; k < new_dts.length; k++) {
//
//                    }
                    for (int k = 0; k < n; k++){
                        byteBuffer = ByteBuffer.allocate(4);
                        byteBuffer.putInt(indices[k]);
                        byteArray = byteBuffer.array();
                        for (byte temp:byteArray){
                            resList.add(temp);
                        }
                        byteBuffer = ByteBuffer.allocate(4);
                        byteBuffer.putFloat((float)values[k].getReal());
                        byteArray = byteBuffer.array();
                        for (byte temp:byteArray){
                            resList.add(temp);
                        }
                        byteBuffer = ByteBuffer.allocate(4);
                        byteBuffer.putFloat((float)values[k].getImaginary());
                        byteArray = byteBuffer.array();
                        for (byte temp:byteArray){
                            resList.add(temp);
                        }
                    }
                    byteBuffer = ByteBuffer.allocate(4);
                    byteBuffer.putInt(encode_pos);
                    byteArray = byteBuffer.array();
                    for (byte temp:byteArray){
                        resList.add(temp);
                    }
                    for (int l = 0; l < encode_pos; l++){
                        byte temp = elems[l];
                        resList.add(temp);
                    }
                } catch (IllegalArgumentException e) {
                    System.out.println(e.getMessage());
                }
            } else {
                int start = i * blockSize;
                int end = Math.min(start + blockSize, timeSeries.length);

                int[] block = new int[end - start];
                System.arraycopy(timeSeries, start, block, 0, end - start);
                //ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                byte[] elems = new byte[block.length*10];
                int encode_pos = 0;
                encode_pos = BOSBlockEncoderImprove(block,0,end - start,end - start,encode_pos,elems);
//                编码block内数据
//                for (int j : block) {
//                    encoder.encode(j, buffer);
//                }
                //byte[] elems = buffer.toByteArray();
                byteBuffer = ByteBuffer.allocate(4);
                byteBuffer.putInt(encode_pos);
                byteArray = byteBuffer.array();
                for (byte temp:byteArray){
                    resList.add(temp);
                }
                for (int l = 0; l < encode_pos; l++){
                    byte temp = elems[l];
                    resList.add(temp);
                }
            }
        }
        return resList;
    }

    public static ArrayList<Integer> decode(ArrayList<Byte> encoded) throws IOException {
        ArrayList<Integer> res = new ArrayList<>();
        int cursor = 0;
        byte[] numByte = new byte[4];
        for (int i = 0; i < 4; i++){
            numByte[i] = encoded.get(cursor++);
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(numByte);
        int numberOfBlocks = byteBuffer.getInt(0);
        for (int i = 0; i < numberOfBlocks; i++) {
            if (i != numberOfBlocks - 1) {
                double[] new_res = new double[blockSize];
                for (int k = 0; k < n; k++){
                    numByte = new byte[4];
                    for (int j = 0; j < 4; j++){
                        numByte[j] = encoded.get(cursor++);
                    }
                    byteBuffer = ByteBuffer.wrap(numByte);
                    int index = byteBuffer.getInt(0);
                    numByte = new byte[4];
                    for (int j = 0; j < 4; j++){
                        numByte[j] = encoded.get(cursor++);
                    }
                    byteBuffer = ByteBuffer.wrap(numByte);
                    float value = byteBuffer.getFloat(0);
                    new_res[index] = (double)value;
                }
                double[] new_dts = dctTransformer.transform(new_res, TransformType.INVERSE);
                int[] new_its = new int[new_dts.length];
                for (int k = 0; k < new_dts.length; k++) {
                    new_its[k] = (int) Math.round(new_dts[k]);
                }
                numByte = new byte[4];
                for (int j = 0; j < 4; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                int length = byteBuffer.getInt(0);
                numByte = new byte[length];
                for (int j = 0; j < length; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                int j = 0;
                //从bytebuffer中解码出残差temp ，res = new_its[j++] - temp
//                while (decoder.hasNext(byteBuffer)) {
//                    int temp = decoder.readInt(byteBuffer);
//                    res.add(new_its[j++] - temp);
//                }
            } else {
                numByte = new byte[4];
                for (int j = 0; j < 4; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);
                int length = byteBuffer.getInt(0);
                numByte = new byte[length];
                for (int j = 0; j < length; j++){
                    numByte[j] = encoded.get(cursor++);
                }
                byteBuffer = ByteBuffer.wrap(numByte);

//                while (decoder.hasNext(byteBuffer)) {
//                    int temp = decoder.readInt(byteBuffer);
//                    res.add(temp);
//                }
            }
        }
        return res;
    }

    public static void main1(String[] args) throws IOException {}

    @Test
    public void fftTest() throws IOException {
//        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "icde0802/supply_experiment/R3O2_compare_compression/compression_ratio/fft_comp";
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
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
//        dataset_block_size.add(1024);

        int repeatTime2 = 100;
//        for (int file_i = 8; file_i < 9; file_i++) {
        long compressTime = 0;
        long uncompressTime = 0;
        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
//        for (int file_i = input_path_list.size()-1; file_i >=0 ; file_i--) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
//                    "Compress Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data2 = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
//                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[0]));
                }
                inputStream.close();

                int[] data2_arr = new int[data2.size()];
                for(int i = 0;i<data2.size();i++){
                    data2_arr[i] = data2.get(i);
                }
                double ratio = 0;
                double compressed_size = 0;
                long s = System.nanoTime();
                ArrayList<Byte> res = new ArrayList<>();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    res = FFT_IFFT.encode(data2_arr);
                }
                long e = System.nanoTime();
                compressTime += ((e - s) / repeatTime2);

                // test compression ratio and compressed size
                compressed_size += res.size();
                double ratioTmp = compressed_size / (double) (data2.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
//                for (int repeat = 0; repeat < repeatTime2; repeat++){
//                    ArrayList<Integer> b = DCT_IDCT.decode(res);
//                }
                e = System.nanoTime();
                uncompressTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "FFT",
                        String.valueOf(compressTime),
                        String.valueOf(uncompressTime),
                        String.valueOf(data2.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
            }
            writer.close();
        }
    }

    @Test
    public void fftTest2() throws IOException {
//        String parent_dir = "/Users/xiaojinzhao/Documents/GitHub/encoding-outlier/";// your data path
        String parent_dir = "/Users/zihanguo/Downloads/R/outlier/outliier_code/encoding-outlier/";
        String output_parent_dir = parent_dir + "icde0802/supply_experiment/R3O2_compare_compression/compression_ratio/fft_bos_comp";
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
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10
//        dataset_block_size.add(2048);
        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11
//        dataset_block_size.add(1024);

        int repeatTime2 = 50;
//        for (int file_i = 8; file_i < 9; file_i++) {
        long compressTime = 0;
        long uncompressTime = 0;
        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
//        for (int file_i = input_path_list.size()-1; file_i >=0 ; file_i--) {

            String inputPath = input_path_list.get(file_i);
            System.out.println(inputPath);
            String Output = output_path_list.get(file_i);

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
//                    "Compress Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data2 = new ArrayList<>();

                loader.readHeaders();
                while (loader.readRecord()) {
//                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }
                inputStream.close();

                int[] data2_arr = new int[data2.size()];
                for(int i = 0;i<data2.size();i++){
                    data2_arr[i] = data2.get(i);
                }
                double ratio = 0;
                double compressed_size = 0;
                long s = System.nanoTime();
                ArrayList<Byte> res = new ArrayList<>();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    res = FFT_IFFT.encode2(data2_arr);
                }
                long e = System.nanoTime();
                compressTime += ((e - s) / repeatTime2);

                // test compression ratio and compressed size
                compressed_size += res.size();
                double ratioTmp = compressed_size / (double) (data2.size() * Integer.BYTES);
                ratio += ratioTmp;
                s = System.nanoTime();
//                for (int repeat = 0; repeat < repeatTime2; repeat++){
//                    ArrayList<Integer> b = DCT_IDCT.decode(res);
//                }
                e = System.nanoTime();
                uncompressTime += ((e - s) / repeatTime2);


                String[] record = {
                        f.toString(),
                        "BOS+FFT",
                        String.valueOf(compressTime),
                        String.valueOf(uncompressTime),
                        String.valueOf(data2.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                writer.writeRecord(record);
                System.out.println(ratio);
            }
            writer.close();
        }
    }
}