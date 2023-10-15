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

public class BOSAmortizationTest {

    public static int getBitWith(int num) {
        if (num == 0) return 1;
        else return 32 - Integer.numberOfLeadingZeros(num);
    }


    public static void int2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
    }


    public static void intByte2Bytes(int integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer);
    }


    private static void long2intBytes(long integer, int encode_pos, byte[] cur_byte) {
        cur_byte[encode_pos] = (byte) (integer >> 24);
        cur_byte[encode_pos + 1] = (byte) (integer >> 16);
        cur_byte[encode_pos + 2] = (byte) (integer >> 8);
        cur_byte[encode_pos + 3] = (byte) (integer);
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

    public static void pack8Values(ArrayList<Integer> values, int offset, int width, int encode_pos, byte[] encoded_result) {
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
                buffer |= (values.get(valueIdx) << (leftSize - width));
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
                encode_pos++;
                bufIdx++;
                if (bufIdx >= width) {
                    return;
                }
            }
        }
    }

    public static void unpack8Values(byte[] encoded, int offset, int width, ArrayList<Integer> result_list) {
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
                result_list.add((int) (buffer >>> (totalBits - width)));
                valueIdx++;
                totalBits -= width;
                buffer = buffer & ((1L << totalBits) - 1);
            }
        }
    }

    public static int bitPacking(ArrayList<Integer> numbers, int start, int bit_width, int encode_pos, byte[] encoded_result) {
        int block_num = (numbers.size() - start) / 8;
        for (int i = 0; i < block_num; i++) {
            pack8Values(numbers, start + i * 8, bit_width, encode_pos, encoded_result);
            encode_pos += bit_width;
        }

        return encode_pos;

    }

    public static ArrayList<Integer> decodeBitPacking(
            byte[] encoded, int decode_pos, int bit_width, int block_size) {
        ArrayList<Integer> result_list = new ArrayList<>();
        int block_num = (block_size - 1) / 8;

        for (int i = 0; i < block_num; i++) { // bitpacking
            unpack8Values(encoded, decode_pos, bit_width, result_list);
            decode_pos += bit_width;

        }
        return result_list;
    }


    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            ArrayList<Integer> min_delta, int supple_length) {
        int block_size = ts_block.length - 1;
        int[] ts_block_delta = new int[ts_block.length + supple_length - 1];

        int value_delta_min = Integer.MAX_VALUE;
        for (int i = 0; i < block_size; i++) {

            int epsilon_v = ts_block[i + 1] - ts_block[i];

            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }

        }
        min_delta.add(ts_block[0]);
        min_delta.add(value_delta_min);

        for (int i = 0; i < block_size; i++) {
            int epsilon_v = ts_block[i + 1] - value_delta_min - ts_block[i];
            ts_block_delta[i] = epsilon_v;
        }
        for (int i = block_size; i < block_size + supple_length; i++) {
            ts_block_delta[i] = 0;
        }
        return ts_block_delta;
    }

    public static int[] getAbsDeltaTsBlock(
            int[] ts_block,
            int i,
            int block_size,
            ArrayList<Integer> min_delta) {
        int[] ts_block_delta = new int[block_size - 1];

        int value_delta_min = Integer.MAX_VALUE;
        for (int j = i * block_size + 1; j < (i + 1) * block_size; j++) {

            int epsilon_v = ts_block[j] - ts_block[j - 1];

            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }

        }
        min_delta.add(ts_block[i * block_size]);
        min_delta.add(value_delta_min);

        int base = i * block_size + 1;
        int end = (i + 1) * block_size;
        for (int j = base; j < end; j++) {
            int epsilon_v = ts_block[j] - value_delta_min - ts_block[j - 1];
            ts_block_delta[j - base] = epsilon_v;
        }
        return ts_block_delta;
    }


    public static int encodeOutlier2Bytes(
            ArrayList<Integer> ts_block_delta,
            int bit_width,
            int encode_pos, byte[] encoded_result) {

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
                long2intBytes(cur_remaining, encode_pos, encoded_result);
                encode_pos += 4;

                cur_remaining = 0;
                cur_number_bits = 0;
            }

            cur_remaining <<= cur_bit_width;
            cur_number_bits += cur_bit_width;
            cur_remaining += (((cur_value << (32 - cur_bit_width)) & 0xFFFFFFFFL) >> (32 - cur_bit_width)); //
        }
        cur_remaining <<= (32 - cur_number_bits);
        long2intBytes(cur_remaining, encode_pos, encoded_result);
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
                                     int final_left_max,
                                     int final_k_start_value,
                                     int final_k_end_value,
                                     int max_delta_value,
                                     ArrayList<Integer> min_delta,
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

            index_bitmap_outlier <<= (8 - cur_index_bitmap_outlier_bits % 8);
            index_bitmap_outlier &= 0xFF;
            bitmap_outlier.add(index_bitmap_outlier);
        }

        int final_alpha = ((k1 + k2) * getBitWith(block_size)) <= (block_size + k1 + k2) ? 1 : 0;


        int k_byte = (k1 << 1);
        k_byte += final_alpha;
        k_byte += (k2 << 16);


        int2Bytes(k_byte, encode_pos, cur_byte);
        encode_pos += 4;

        int2Bytes(min_delta.get(0), encode_pos, cur_byte);
        encode_pos += 4;
        int2Bytes(min_delta.get(1), encode_pos, cur_byte);
        encode_pos += 4;
        int2Bytes(final_k_start_value, encode_pos, cur_byte);
        encode_pos += 4;
        int bit_width_final = getBitWith(final_k_end_value - final_k_start_value);
        intByte2Bytes(bit_width_final, encode_pos, cur_byte);
        encode_pos += 1;
        int left_bit_width = getBitWith(final_left_max);
        int right_bit_width = getBitWith(max_delta_value - final_k_end_value);
        intByte2Bytes(left_bit_width, encode_pos, cur_byte);
        encode_pos += 1;
        intByte2Bytes(right_bit_width, encode_pos, cur_byte);
        encode_pos += 1;
        if (final_alpha == 0) {

            for (int i : bitmap_outlier) {

                intByte2Bytes(i, encode_pos, cur_byte);
                encode_pos += 1;
            }
        } else {
            encode_pos = encodeOutlier2Bytes(final_left_outlier_index, getBitWith(block_size), encode_pos, cur_byte);
            encode_pos = encodeOutlier2Bytes(final_right_outlier_index, getBitWith(block_size), encode_pos, cur_byte);
        }
        encode_pos = encodeOutlier2Bytes(final_normal, bit_width_final, encode_pos, cur_byte);
        if (k1 != 0)
            encode_pos = encodeOutlier2Bytes(final_left_outlier, left_bit_width, encode_pos, cur_byte);
        if (k2 != 0)
            encode_pos = encodeOutlier2Bytes(final_right_outlier, right_bit_width, encode_pos, cur_byte);
        return encode_pos;

    }

    private static int BOSBlockEncoder(int[] ts_block, int block_i, int block_size, int q, int k, int encode_pos, byte[] cur_byte) {


        ArrayList<Integer> min_delta = new ArrayList<>();
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, block_i, block_size, min_delta);

        block_size--;

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

        if (sigma == 0) {
            encode_pos = BOSEncodeBits(ts_block_delta, 0, 0, 0, final_right_max,
                    min_delta, encode_pos, cur_byte);
            return encode_pos;
        }
        final_right_max = ((final_right_max + 1) / q + 1) * q;
        int final_up_bound = ((int) ((mu + k * sigma) / q + 1) * q);

        int max_delta_value_bit_width = getBitWith(final_up_bound);

        int final_k_start_value = 0;
        int final_k_end_value = final_right_max;
        ArrayList<Integer> spread_value = new ArrayList<>();
        int beta_start = (int) Math.floor(Math.log(sigma));
        for (int i = beta_start; i < max_delta_value_bit_width; i++) {
            int spread_v = (int) pow(2, i) - 1;
            spread_value.add(spread_v);
        }

        int min_bits = 0;
        min_bits += (getBitWith(final_k_end_value - final_k_start_value) * (block_size - 1));

        // Divided into buckets
        int bucket_num = (int) Math.ceil(((double) final_right_max + 1) / (double) q);
        int[] bucket_count = new int[bucket_num];
        for (int i = 0; i < bucket_num; i++) {
            bucket_count[i] = 0;
        }
        for (int i = 0; i < block_size; i++) {
            int tmp_i = ts_block_delta[i] / q;
            bucket_count[tmp_i]++;
        }
        int[] PDF = new int[bucket_num];
        int cumulative = 0;
        int bucket_start_i = (int) (Math.max((mu - k * sigma) / q + 1, 0));
        int bucket_end_i = (int) (Math.min((mu + k * sigma) / q - 1, bucket_num));
        for (int i = 0; i < bucket_num; i++) {
            int count = bucket_count[i];
            PDF[i] = cumulative;
            cumulative += count;
        }
        PDF[bucket_num - 1] = cumulative;
        for (int i = bucket_start_i; i < bucket_end_i; i++) {
            if (bucket_count[i] == 0) {
                continue;
            }
            int k_start_value = i * q;
            for (int k_spread_value : spread_value) {
                int k_end_value = Math.min(k_spread_value + k_start_value, final_right_max);

                int cur_bits = 0;
                int cur_k1 = PDF[i];

                int k_end_value_bucket = k_end_value / q;
                k_end_value = k_end_value_bucket * q;

                int cur_k2;
                if (k_end_value_bucket != bucket_num - 1)
                    cur_k2 = block_size - PDF[k_end_value_bucket + 1];
                else cur_k2 = block_size - PDF[k_end_value_bucket];


                cur_bits += Math.min((cur_k1 + cur_k2) * getBitWith(block_size), block_size + cur_k1 + cur_k2);
                if (cur_k1 != 0)
                    cur_bits += cur_k1 * getBitWith(k_start_value);
                if (cur_k1 + cur_k2 != block_size)
                    cur_bits += (block_size - cur_k1 - cur_k2) * getBitWith(k_end_value - k_start_value);
                if (cur_k2 != 0)
                    cur_bits += cur_k2 * getBitWith(final_right_max - k_end_value);


                if (cur_bits < min_bits) {
                    min_bits = cur_bits;
                    final_k_start_value = k_start_value;
                    final_k_end_value = k_end_value;
                }
                if (k_end_value == final_up_bound)
                    break;
            }
        }

        encode_pos = BOSEncodeBits(ts_block_delta, final_k_start_value, final_k_start_value, final_k_end_value, final_right_max,
                min_delta, encode_pos, cur_byte);

        return encode_pos;


    }

    private static int BOSBlockEncoder(int[] ts_block, int supple_length, int q, int k, int encode_pos, byte[] cur_byte) {

        int block_size = ts_block.length - 1;

        ArrayList<Integer> min_delta = new ArrayList<>();
        int[] ts_block_delta = getAbsDeltaTsBlock(ts_block, min_delta, supple_length);


        block_size = block_size + supple_length;

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

        if (sigma == 0) {
            encode_pos = BOSEncodeBits(ts_block_delta, 0, 0, 0, final_right_max,
                    min_delta, encode_pos, cur_byte);
            return encode_pos;
        }

        final_right_max = ((final_right_max + 1) / q + 1) * q;
        int final_up_bound = ((int) ((mu + k * sigma) / q + 1) * q);

        int max_delta_value_bit_width = getBitWith(final_up_bound);


        int final_k_start_value = 0;
        int final_k_end_value = final_right_max;
        ArrayList<Integer> spread_value = new ArrayList<>();
        for (int i = (int) Math.floor(Math.log(sigma)); i < max_delta_value_bit_width; i++) {
            int spread_v = (int) pow(2, i) - 1;
            spread_value.add(spread_v);
        }

        int min_bits = 0;
        min_bits += (getBitWith(final_k_end_value - final_k_start_value) * (block_size - 1));

        // Divided into buckets
        int bucket_num = (int) Math.ceil(((double) final_right_max + 1) / (double) q);
        int[] bucket_count = new int[bucket_num];
        for (int i = 0; i < bucket_num; i++) {
            bucket_count[i] = 0;
        }
        for (int i = 0; i < block_size; i++) {
            int tmp_i = ts_block_delta[i] / q;
            bucket_count[tmp_i]++;
        }
        int[] PDF = new int[bucket_num];
        int cumulative = 0;
        int bucket_start_i = (int) (Math.max((mu - k * sigma) / q + 1, 0));
        int bucket_end_i = (int) (Math.min((mu + k * sigma) / q - 1, bucket_num));
        for (int i = 0; i < bucket_num; i++) {
            int count = bucket_count[i];
            PDF[i] = cumulative;
            cumulative += count;
        }
        PDF[bucket_num - 1] = cumulative;
        for (int i = bucket_start_i; i < bucket_end_i; i++) {
            if (bucket_count[i] == 0) {
                continue;
            }
            int k_start_value = i * q;
            for (int k_spread_value : spread_value) {
                int k_end_value = Math.min(k_spread_value + k_start_value, final_right_max);

                int cur_bits = 0;
                int cur_k1 = PDF[i];

                int k_end_value_bucket = k_end_value / q;
                k_end_value = k_end_value_bucket * q;

                int cur_k2;
                if (k_end_value_bucket != bucket_num - 1)
                    cur_k2 = block_size - PDF[k_end_value_bucket + 1];
                else cur_k2 = block_size - PDF[k_end_value_bucket];


                cur_bits += Math.min((cur_k1 + cur_k2) * getBitWith(block_size), block_size + cur_k1 + cur_k2);
                if (cur_k1 != 0)
                    cur_bits += cur_k1 * getBitWith(k_start_value);
                if (cur_k1 + cur_k2 != block_size)
                    cur_bits += (block_size - cur_k1 - cur_k2) * getBitWith(k_end_value - k_start_value);
                if (cur_k2 != 0)
                    cur_bits += cur_k2 * getBitWith(final_right_max - k_end_value);


                if (cur_bits < min_bits) {
                    min_bits = cur_bits;
                    final_k_start_value = k_start_value;
                    final_k_end_value = k_end_value;
                }
                if (k_end_value == final_up_bound)
                    break;
            }
        }


        encode_pos = BOSEncodeBits(ts_block_delta, final_k_start_value, final_k_start_value, final_k_end_value, final_right_max,
                min_delta, encode_pos, cur_byte);

        return encode_pos;
    }

    public static int BOSEncoder(
            int[] data, int block_size, int q, int k, byte[] encoded_result) {
        block_size++;


        int length_all = data.length;

        int encode_pos = 0;
        int2Bytes(length_all, encode_pos, encoded_result);
        encode_pos += 4;

        int block_num = length_all / block_size;
        int2Bytes(block_size, encode_pos, encoded_result);
        encode_pos += 4;

        for (int i = 0; i < block_num; i++) {

            encode_pos = BOSBlockEncoder(data, i, block_size, q, k, encode_pos, encoded_result);

        }

        int remaining_length = length_all - block_num * block_size;
        if (remaining_length <= 3) {
            for (int i = remaining_length; i > 0; i--) {
                int2Bytes(data[data.length - i], encode_pos, encoded_result);
                encode_pos += 4;
            }

        } else {

            int start = block_num * block_size;
            int[] ts_block = new int[length_all - start];
            if (length_all - start >= 0) System.arraycopy(data, start, ts_block, 0, length_all - start);

            int supple_length;
            if (remaining_length % 8 == 0) {
                supple_length = 1;
            } else if (remaining_length % 8 == 1) {
                supple_length = 0;
            } else {
                supple_length = 9 - remaining_length % 8;
            }


            encode_pos = BOSBlockEncoder(ts_block, supple_length, q, k, encode_pos, encoded_result);

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
        value_list[value_pos_arr[0]] = value0;
        value_pos_arr[0]++;

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
        ArrayList<Integer> final_normal;
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();

        if (final_alpha == 0) {
            int bitmap_bytes = (int) Math.ceil((double) (block_size - 1 + k1 + k2) / (double) 8);
            for (int i = 0; i < bitmap_bytes; i++) {
                bitmap_outlier.add(bytes2Integer(encoded, decode_pos, 1));
                decode_pos += 1;
            }
            int bitmap_outlier_i = 0;
            int remaining_bits = 8;
            int tmp = bitmap_outlier.get(bitmap_outlier_i);
            bitmap_outlier_i++;
            int i = 0;
            while (i < block_size - 1) {
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
            final_left_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(block_size), k1, decode_pos_result_left);
            decode_pos = (decode_pos_result_left.get(0));
            ArrayList<Integer> decode_pos_result_right = new ArrayList<>();
            final_right_outlier_index = decodeOutlier2Bytes(encoded, decode_pos, getBitWith(block_size), k2, decode_pos_result_right);
            decode_pos = (decode_pos_result_right.get(0));
        }


        ArrayList<Integer> decode_pos_normal = new ArrayList<>();
        final_normal = decodeOutlier2Bytes(encoded, decode_pos, bit_width_final, block_size - k1 - k2 - 1, decode_pos_normal);

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


        for (int i = 0; i < block_size - 1; i++) {
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
        int zero_number;
        if (remain_length % 8 == 0) {
            zero_number = 1;
        } else if (remain_length % 8 == 1) {
            zero_number = 0;
        } else {
            zero_number = 9 - remain_length % 8;
        }

        int[] value_list = new int[length_all + 8];

        int[] value_pos_arr = new int[1];

        for (int k = 0; k < block_num; k++) {
            decode_pos = BOSBlockDecoder(encoded, decode_pos, value_list, block_size, value_pos_arr);
        }

        if (remain_length <= 3) {
            for (int i = 0; i < remain_length; i++) {
                int value_end = bytes2Integer(encoded, decode_pos, 4);
                decode_pos += 4;
                value_list[value_pos_arr[0]] = value_end;
                value_pos_arr[0]++;
            }
        } else {
            BOSBlockDecoder(encoded, decode_pos, value_list, remain_length + zero_number, value_pos_arr);
        }
    }

    @Test
    public void BOSAmortization() throws IOException {
        String parent_dir = "iotdb/iotdb-core/tsfile/src/test/resources/"; // your data path
        String output_parent_dir = parent_dir + "bos_amortization";
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
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();


                loader.readHeaders();
                while (loader.readRecord()) {
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }
                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for (int i = 0; i < data2.size(); i++) {
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length * 4];
                long encodeTime = 0;
                long decodeTime = 0;
                double ratio = 0;
                double compressed_size = 0;
                int repeatTime2 = 500;

                int length = 0;

                long s = System.nanoTime();
                for (int repeat = 0; repeat < repeatTime2; repeat++) {
                    length = BOSEncoder(data2_arr, dataset_block_size.get(file_i), 4, 1, encoded_result);
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
                        "TS_2DIFF+BOS-A",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data1.size()),
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
    public void BOSAmortizationVaryBlockSize() throws IOException {
        String parent_dir = "iotdb/iotdb-core/tsfile/src/test/resources/"; // your data path
        String output_parent_dir = parent_dir + "block_size_amortization";
        String input_parent_dir = parent_dir + "trans_data/";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();

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
        int repeatTime2 = 500;

        for (String value : dataset_name) {
            input_path_list.add(input_parent_dir + value);
        }

        output_path_list.add(output_parent_dir + "/CS-Sensors_ratio.csv"); // 0

        output_path_list.add(output_parent_dir + "/Metro-Traffic_ratio.csv");// 1

        output_path_list.add(output_parent_dir + "/USGS-Earthquakes_ratio.csv");// 2

        output_path_list.add(output_parent_dir + "/YZ-Electricity_ratio.csv"); // 3

        output_path_list.add(output_parent_dir + "/GW-Magnetic_ratio.csv"); //4

        output_path_list.add(output_parent_dir + "/TY-Fuel_ratio.csv");//5

        output_path_list.add(output_parent_dir + "/Cyber-Vehicle_ratio.csv"); //6

        output_path_list.add(output_parent_dir + "/Vehicle-Charge_ratio.csv");//7

        output_path_list.add(output_parent_dir + "/Nifty-Stocks_ratio.csv");//8

        output_path_list.add(output_parent_dir + "/TH-Climate_ratio.csv");//9

        output_path_list.add(output_parent_dir + "/TY-Transport_ratio.csv");//10

        output_path_list.add(output_parent_dir + "/EPM-Education_ratio.csv");//11


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
                    "Block Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();


                loader.readHeaders();

                while (loader.readRecord()) {

                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));

                }

                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for (int i = 0; i < data2.size(); i++) {
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length * 4];

                for (int block_size_i = 13; block_size_i > 3; block_size_i--) {
                    int block_size = (int) Math.pow(2, block_size_i);
                    System.out.println(block_size);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    double ratio = 0;
                    double compressed_size = 0;

                    long s = System.nanoTime();
                    for (int repeat = 0; repeat < repeatTime2; repeat++) {
                        compressed_size = BOSEncoder(data2_arr, block_size, 4, 1, encoded_result);
                    }

                    long e = System.nanoTime();
                    encodeTime += ((e - s) / repeatTime2);

                    double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                    ratio += ratioTmp;
                    s = System.nanoTime();
                    for (int repeat = 0; repeat < repeatTime2; repeat++)
                        BOSDecoder(encoded_result);
                    e = System.nanoTime();
                    decodeTime += ((e - s) / repeatTime2);

                    String[] record = {
                            f.toString(),
                            "TS_2DIFF+BOS-A",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data1.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(block_size_i),
                            String.valueOf(ratio)
                    };
                    writer.writeRecord(record);
                    System.out.println(ratio);
                }

            }
            writer.close();

        }
    }

    @Test
    public void BOSAmortizationVaryK() throws IOException {
        String parent_dir = "iotdb/iotdb-core/tsfile/src/test/resources/"; // your data path
        String output_parent_dir = parent_dir + "vary_k_amortization";
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
                    "k",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {
                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();


                loader.readHeaders();

                while (loader.readRecord()) {
                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }
                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for (int i = 0; i < data2.size(); i++) {
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length * 4];
                for (int k = 1; k < 8; k++) {
                    System.out.println(k);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    double ratio = 0;
                    double compressed_size = 0;
                    int repeatTime2 = 10;

                    long s = System.nanoTime();

                    for (int repeat = 0; repeat < repeatTime2; repeat++) {

                        compressed_size = BOSEncoder(data2_arr, dataset_block_size.get(file_i), 4, k, encoded_result);
                    }

                    long e = System.nanoTime();
                    encodeTime += ((e - s) / repeatTime2);

                    double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                    ratio += ratioTmp;
                    s = System.nanoTime();
                    for (int repeat = 0; repeat < repeatTime2; repeat++)
                        BOSDecoder(encoded_result);
                    e = System.nanoTime();
                    decodeTime += ((e - s) / repeatTime2);


                    String[] record = {
                            f.toString(),
                            "TS_2DIFF+BOS-A",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data1.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(k),
                            String.valueOf(ratio)
                    };
                    writer.writeRecord(record);
                    System.out.println(ratio);
                }

            }
            writer.close();

        }
    }

    @Test
    public void BOSAmortizationVaryQ() throws IOException {
        String parent_dir = "iotdb/iotdb-core/tsfile/src/test/resources/"; // your data path
        String output_parent_dir = parent_dir + "vary_q_amortization";
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
                    "Q",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {

                System.out.println(f);
                InputStream inputStream = Files.newInputStream(f.toPath());

                CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                ArrayList<Integer> data1 = new ArrayList<>();
                ArrayList<Integer> data2 = new ArrayList<>();


                loader.readHeaders();

                while (loader.readRecord()) {

                    data1.add(Integer.valueOf(loader.getValues()[0]));
                    data2.add(Integer.valueOf(loader.getValues()[1]));
                }

                inputStream.close();
                int[] data2_arr = new int[data1.size()];
                for (int i = 0; i < data2.size(); i++) {
                    data2_arr[i] = data2.get(i);
                }
                byte[] encoded_result = new byte[data2_arr.length * 4];
                for (int q_exp = 0; q_exp < 8; q_exp++) {
                    int q = (int) Math.pow(2, q_exp);
                    System.out.println(q);
                    long encodeTime = 0;
                    long decodeTime = 0;
                    double ratio = 0;
                    double compressed_size = 0;
                    int repeatTime2 = 500;
                    long s = System.nanoTime();

                    for (int repeat = 0; repeat < repeatTime2; repeat++) {
                        compressed_size = BOSEncoder(data2_arr, dataset_block_size.get(file_i), q, 1, encoded_result);
                    }

                    long e = System.nanoTime();
                    encodeTime += ((e - s) / repeatTime2);

                    double ratioTmp = compressed_size / (double) (data1.size() * Integer.BYTES);
                    ratio += ratioTmp;
                    s = System.nanoTime();
                    for (int repeat = 0; repeat < repeatTime2; repeat++)
                        BOSDecoder(encoded_result);
                    e = System.nanoTime();
                    decodeTime += ((e - s) / repeatTime2);


                    String[] record = {
                            f.toString(),
                            "TS_2DIFF+BOS-A",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data1.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(q_exp),
                            String.valueOf(ratio)
                    };
                    writer.writeRecord(record);
                    System.out.println(ratio);
                }

            }
            writer.close();

        }
    }

}
