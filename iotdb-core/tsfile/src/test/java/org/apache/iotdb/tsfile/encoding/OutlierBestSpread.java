package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Stack;

public class OutlierBestSpread {

    public static int getBitWith(int num) {
        if (num == 0) return 1;
        else return 32 - Integer.numberOfLeadingZeros(num);
    }

    public static byte[] int2Bytes(int integer) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) (integer >> 24);
        bytes[1] = (byte) (integer >> 16);
        bytes[2] = (byte) (integer >> 8);
        bytes[3] = (byte) integer;
        return bytes;
    }

    public static byte[] intByte2Bytes(int integer) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) integer;
        return bytes;
    }

    public static byte[] double2Bytes(double dou) {
        long value = Double.doubleToRawLongBits(dou);
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) ((value >> 8 * i) & 0xff);
        }
        return bytes;
    }

    public static double bytes2Double(ArrayList<Byte> encoded, int start, int num) {
        if (num > 8) {
            System.out.println("bytes2Doubleerror");
            return 0;
        }
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (encoded.get(i + start) & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }

    public static byte[] float2bytes(float f) {
        int fbit = Float.floatToIntBits(f);
        byte[] b = new byte[4];
        for (int i = 0; i < 4; i++) {
            b[i] = (byte) (fbit >> (24 - i * 8));
        }
        int len = b.length;
        byte[] dest = new byte[len];
        System.arraycopy(b, 0, dest, 0, len);
        byte temp;
        for (int i = 0; i < len / 2; ++i) {
            temp = dest[i];
            dest[i] = dest[len - i - 1];
            dest[len - i - 1] = temp;
        }
        return dest;
    }

    public static float bytes2float(ArrayList<Byte> b, int index) {
        int l;
        l = b.get(index);
        l &= 0xff;
        l |= ((long) b.get(index + 1) << 8);
        l &= 0xffff;
        l |= ((long) b.get(index + 2) << 16);
        l &= 0xffffff;
        l |= ((long) b.get(index + 3) << 24);
        return Float.intBitsToFloat(l);
    }

    public static int bytes2Integer(ArrayList<Byte> encoded, int start, int num) {
        int value = 0;
        if (num > 4) {
            System.out.println("bytes2Integer error");
            return 0;
        }
        for (int i = 0; i < num; i++) {
            value <<= 8;
            int b = encoded.get(i + start) & 0xFF;
            value |= b;
        }
        return value;
    }

    public static byte[] bitPacking(ArrayList<Integer> numbers, int start, int bit_width) {
        int block_num = numbers.size() / 8;
        byte[] result = new byte[bit_width * block_num];
        for (int i = 0; i < block_num; i++) {
            for (int j = 0; j < bit_width; j++) {
                int tmp_int = 0;
                for (int k = 0; k < 8; k++) {
                    tmp_int += (((numbers.get(i * 8 + k + start) >> j) % 2) << k);
                }
                result[i * bit_width + j] = (byte) tmp_int;
            }
        }
        return result;
    }

    public static byte[] bitPacking(ArrayList<ArrayList<Integer>> numbers, int start, int index, int bit_width) {
        int block_num = numbers.size() / 8;
        byte[] result = new byte[bit_width * block_num];
        for (int i = 0; i < block_num; i++) {
            for (int j = 0; j < bit_width; j++) {
                int tmp_int = 0;
                for (int k = 0; k < 8; k++) {
                    tmp_int += (((numbers.get(i * 8 + k + start).get(index) >> j) % 2) << k);
                }
                result[i * bit_width + j] = (byte) tmp_int;
            }
        }
        return result;
    }

    public static ArrayList<Integer> decodebitPacking(
            ArrayList<Byte> encoded, int decode_pos, int bit_width, int min_delta, int block_size) {
        ArrayList<Integer> result_list = new ArrayList<>();
        for (int i = 0; i < (block_size - 1) / 8; i++) { // bitpacking  纵向8个，bit width是多少列
            int[] val8 = new int[8];
            for (int j = 0; j < 8; j++) {
                val8[j] = 0;
            }
            for (int j = 0; j < bit_width; j++) {
                byte tmp_byte = encoded.get(decode_pos + bit_width - 1 - j);
                byte[] bit8 = new byte[8];
                for (int k = 0; k < 8; k++) {
                    bit8[k] = (byte) (tmp_byte & 1);
                    tmp_byte = (byte) (tmp_byte >> 1);
                }
                for (int k = 0; k < 8; k++) {
                    val8[k] = val8[k] * 2 + bit8[k];
                }
            }
            for (int j = 0; j < 8; j++) {
                result_list.add(val8[j] + min_delta);
            }
            decode_pos += bit_width;
        }
        return result_list;
    }

    public static int part(ArrayList<Integer> arr, int low, int high) {
        int tmp = arr.get(low);
        while (low < high) {
            while (low < high && (arr.get(high) > tmp)) {
                high--;
            }
            arr.set(low, arr.get(high));
            while (low < high && (arr.get(low) < tmp)) {
                low++;
            }
            arr.set(high, arr.get(low));
        }
        arr.set(low, tmp);
        return low;
    }

    public static void quickSort(ArrayList<Integer> arr, int index, int low, int high) {
        Stack<Integer> stack = new Stack<>();
        while (!stack.empty()) {
            high = stack.pop();
            low = stack.pop();
            int mid = part(arr, low, high);

            if (mid + 1 < high) {
                stack.push(mid + 1);
                stack.push(high);
            }
            if (mid - 1 > low) {
                stack.push(low);
                stack.push(mid - 1);
            }
        }
    }

    public static int getCommon(int m, int n) {
        int z;
        while (m % n != 0) {
            z = m % n;
            m = n;
            n = z;
        }
        return n;
    }

    public static void splitTimeStamp3(
            ArrayList<Integer> ts_block, ArrayList<Integer> result) {
        int td_common = 0;
        for (int i = 1; i < ts_block.size(); i++) {
            int time_diffi = ts_block.get(i) - ts_block.get(i - 1);
            if (td_common == 0) {
                if (time_diffi != 0) {
                    td_common = time_diffi;
                    continue;
                } else {
                    continue;
                }
            }
            if (time_diffi != 0) {
                td_common = getCommon(time_diffi, td_common);
                if (td_common == 1) {
                    break;
                }
            }
        }
        if (td_common == 0) {
            td_common = 1;
        }

        int t0 = ts_block.get(0);
        for (int i = 0; i < ts_block.size(); i++) {
            int interval_i = (ts_block.get(i) - t0) / td_common;
            ts_block.set(i, t0 + interval_i);
        }
        result.add(td_common);
    }


    public static ArrayList<Integer> getAbsDeltaTsBlock(
            ArrayList<Integer> ts_block,
            ArrayList<Integer> min_delta) {
        ArrayList<Integer> ts_block_delta = new ArrayList<>();

        ts_block_delta.add(ts_block.get(0));
        int value_delta_min = Integer.MAX_VALUE;
        for (int i = 1; i < ts_block.size(); i++) {

            int epsilon_v = ts_block.get(i) - ts_block.get(i - 1);

            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }

        }
        min_delta.add(value_delta_min);
        for (int i = 1; i < ts_block.size(); i++) {
            int epsilon_v = ts_block.get(i) - value_delta_min - ts_block.get(i - 1);
            ts_block_delta.add(epsilon_v);
        }
        return ts_block_delta;
    }

    public static ArrayList<Integer> getBitWith(ArrayList<Integer> ts_block) {
        ArrayList<Integer> ts_block_bit_width = new ArrayList<>();
        for (int integers : ts_block) {
            ts_block_bit_width.add(getBitWith(integers));
        }
        return ts_block_bit_width;
    }

    public static ArrayList<Byte> encode2Bytes(
            ArrayList<Integer> ts_block,
            int min_delta,
            int bit_width) {
        ArrayList<Byte> encoded_result = new ArrayList<>();

        // encode value0
        byte[] value0_byte = int2Bytes(ts_block.get(0));
        for (byte b : value0_byte) encoded_result.add(b);

        // encode theta
        byte[] value_min_byte = int2Bytes(min_delta);
        for (byte b : value_min_byte) encoded_result.add(b);

        // encode value
        byte[] max_bit_width_value_byte = int2Bytes(bit_width);
        for (byte b : max_bit_width_value_byte) encoded_result.add(b);
        byte[] value_bytes = bitPacking(ts_block, 1, bit_width);
        for (byte b : value_bytes) encoded_result.add(b);


        return encoded_result;
    }

    public static ArrayList<Byte> encodeOutlier2Bytes(
            ArrayList<Integer> ts_block_delta,
            int bit_width) {
        ArrayList<Byte> encoded_result = new ArrayList<>();

//        // encode value0
//        byte[] value0_byte = int2Bytes(ts_block_delta.get(0));
//        for (byte b : value0_byte) encoded_result.add(b);
//

        // encode value
        byte[] value_bytes = bitPacking(ts_block_delta, 0, bit_width);
        for (byte b : value_bytes) encoded_result.add(b);

        int n_k = ts_block_delta.size();
        int n_k_b = n_k / 8;
        int remaining = n_k - n_k_b * 8;

        int cur_remaining = 0;
        int cur_number_bits = 0;
        for (int i = n_k_b * 8; i < n_k; i++) {
            int cur_value = ts_block_delta.get(i);
            int cur_bit_width = bit_width;
            if (cur_number_bits + bit_width > 32) {
                cur_remaining <<= (32 - cur_number_bits);
                cur_bit_width = bit_width - 32 + cur_number_bits;
                cur_remaining += (cur_value >> cur_bit_width);

                byte[] cur_remaining_byte = int2Bytes(cur_remaining);
                for (byte b : cur_remaining_byte) encoded_result.add(b);
                cur_remaining = 0;
            }
            cur_remaining <<= cur_bit_width;
            cur_remaining += ((cur_value << (32 - cur_bit_width)) >> (32 - cur_bit_width));
        }
        byte[] cur_remaining_byte = int2Bytes(cur_remaining);
        for (byte b : cur_remaining_byte) encoded_result.add(b);


        return encoded_result;
    }

    private static int bosEncode(ArrayList<Integer> ts_block, double x_c, int beta, int supple_length) {

//        int block_size = ts_block.size();
        int final_right_max = Integer.MIN_VALUE;
        ArrayList<Byte> cur_byte = new ArrayList<>();

        ArrayList<Integer> min_delta = new ArrayList<>();
        ArrayList<Integer> ts_block_delta = getAbsDeltaTsBlock(ts_block, min_delta);
        for (int s = 0; s < supple_length; s++) {
            ts_block_delta.add(0);
        }

        int block_size = ts_block_delta.size();
        ArrayList<Integer> ts_block_order_value = getAbsDeltaTsBlock(ts_block, min_delta);
        Collections.sort(ts_block_order_value);
//        quickSort(ts_block_order_value, 0, 1, block_size - 1);
//        int bit_width = getBitWith(ts_block_delta.get(block_size - 1));


        double sum = 0;
        for (int i = 1; i < block_size; i++) {
            if (ts_block_delta.get(i) > final_right_max) {
                final_right_max = ts_block_delta.get(i);
            }
            sum += ts_block_delta.get(i);
        }
        double mu = sum / block_size;
        double variance = 0;
        for (int i = 1; i < block_size; i++) {
            variance += (ts_block_delta.get(i) - mu) * (ts_block_delta.get(i) - mu);
        }
        double sigma = Math.sqrt(variance / block_size);

        ArrayList<Integer> PDF = new ArrayList<>();
        int min_delta_value = ts_block_order_value.get(0);
        int tmp = min_delta_value;
        int final_i = 0;


        for (int i = 1; i < block_size; i++) {
            if (ts_block_order_value.get(i) != tmp) {
                int start_v = ts_block_order_value.get(i);
                PDF.add(tmp);
                tmp = start_v;
                final_i = i;
            }
        }
        if (final_i != block_size - 1) {
            PDF.add(tmp);
        }


        int final_k_start_i = (int) ((double) PDF.size() * x_c);
        if (final_k_start_i > PDF.size() - 1) {
            final_k_start_i = PDF.size() - 1;
        }
        int final_k_start_value = PDF.get(final_k_start_i);
        int max_delta = PDF.get(PDF.size() - 1);
        int final_k_end_value = (int) (final_k_start_value + Math.pow(2, beta) > max_delta ? max_delta : (final_k_start_value + Math.pow(2, beta)));
        int cost = 0;

        int k1 = 0;
        int k2 = 0;

        for (int i = 1; i < block_size; i++) {
            if (ts_block_delta.get(i) < final_k_start_value) {
                k1++;
            } else if (ts_block_delta.get(i) > final_k_end_value) {
                k2++;
            }
        }
        int left_bit_width = getBitWith(final_k_start_value);
        int right_bit_width = getBitWith(final_right_max - final_k_end_value);
        cost += left_bit_width * k1;
        cost += right_bit_width * k2;
        cost += beta * (block_size - 1 - k1 - k2);
        cost += Math.min(getBitWith(block_size) * (k1 + k2), (block_size + k1 + k2));

        return cost;
    }

    private static ArrayList<Byte> learnKDelta(ArrayList<Integer> ts_block, int x_c, int beta, int supple_length) {

//        int block_size = ts_block.size();
        int final_right_max = 0;
        ArrayList<Byte> cur_byte = new ArrayList<>();

        ArrayList<Integer> min_delta = new ArrayList<>();
        ArrayList<Integer> ts_block_delta = getAbsDeltaTsBlock(ts_block, min_delta);
        for (int s = 0; s < supple_length; s++) {
            ts_block_delta.add(0);
        }

        int block_size = ts_block_delta.size();
        ArrayList<Integer> ts_block_order_value = getAbsDeltaTsBlock(ts_block, min_delta);
        quickSort(ts_block_order_value, 0, 1, block_size - 1);
//        int bit_width = getBitWith(ts_block_delta.get(block_size - 1));


        double sum = 0;
        for (int i = 1; i < block_size; i++) {
            if (ts_block_delta.get(i) > final_right_max) {
                final_right_max = ts_block_delta.get(i);
            }
            sum += ts_block_delta.get(i);
        }
        double mu = sum / block_size;
        double variance = 0;
        for (int i = 1; i < block_size; i++) {
            variance += (ts_block_delta.get(i) - mu) * (ts_block_delta.get(i) - mu);
        }
        double sigma = Math.sqrt(variance / block_size);

        ArrayList<Integer> PDF = new ArrayList<>();
        int min_delta_value = ts_block_order_value.get(0);
        int tmp = min_delta_value;
        int final_i = 0;


        for (int i = 1; i < block_size; i++) {
            if (ts_block_order_value.get(i) != tmp) {
                int start_v = ts_block_order_value.get(i);
                PDF.add(tmp);
                tmp = start_v;
                final_i = i;
            }
        }
        if (final_i != block_size - 1) {
            PDF.add(tmp);
        }


        int final_k_start_i = x_c >= PDF.size() ? (PDF.size() - 1) : x_c;
        int final_k_start_value = PDF.get(final_k_start_i);
        int max_delta = PDF.get(PDF.size() - 1);
        int final_k_end_value = (int) (final_k_start_value + Math.pow(2, beta) > max_delta ? max_delta : (final_k_start_value + Math.pow(2, beta)));


        // ------------------------- encode data -----------------------------------------

        ArrayList<Integer> final_left_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_right_outlier_index = new ArrayList<>();
        ArrayList<Integer> final_left_outlier = new ArrayList<>();
        ArrayList<Integer> final_right_outlier = new ArrayList<>();
        ArrayList<Integer> final_normal = new ArrayList<>();

        int k1 = 0;
        int k2 = 0;
        ArrayList<Integer> bitmap = new ArrayList<>();
        ArrayList<Integer> bitmap_outlier = new ArrayList<>();
        int index_bitmap = 0;
        int index_bitmap_outlier = 0;
        for (int i = 1; i < block_size; i++) {
            if (ts_block_delta.get(i) < final_k_start_value) {
                final_left_outlier.add(ts_block_delta.get(i));
//                    if (final_alpha == 1)
                final_left_outlier_index.add(i);
//                    else {
                index_bitmap_outlier += 1;
                index_bitmap_outlier <<= 1;
                index_bitmap += 1;
//                    }

                k1++;


            } else if (ts_block_delta.get(i) > final_k_end_value) {
                final_right_outlier.add(ts_block_delta.get(i) - final_k_end_value);

//                    if (final_alpha == 1)
                final_right_outlier_index.add(i);
//                    else {
                index_bitmap_outlier <<= 1;
                index_bitmap += 1;
//                    }
                k2++;
            } else {
                final_normal.add(ts_block_delta.get(i) - final_k_start_value);
            }
//                if (final_alpha == 0) {
            index_bitmap <<= 1;
            if (i % 8 == 0) {
                bitmap.add(index_bitmap);
                index_bitmap = 0;
            }
            if ((k1 + k2) % 8 == 0) {
                bitmap_outlier.add(index_bitmap_outlier);
                index_bitmap_outlier = 0;
            }
//                }
        }
        if ((k1 + k2) % 8 != 0) {
            bitmap_outlier.add(index_bitmap_outlier);
        }
        int final_alpha = 1;
        if (getBitWith(block_size) * (k1 + k2) > (block_size + k1 + k2)) {
            final_alpha = 0;
        }

        k1 <<= 1;
        k1 += final_alpha;
        k1 <<= 16;
        k1 += k2;

        byte[] k1_bytes = int2Bytes(k1);
        for (byte b : k1_bytes) cur_byte.add(b);

        byte[] value0_bytes = int2Bytes(ts_block_delta.get(0));
        for (byte b : value0_bytes) cur_byte.add(b);
        byte[] min_delta_bytes = int2Bytes(min_delta.get(0));
        for (byte b : min_delta_bytes) cur_byte.add(b);

        byte[] final_k_start_value_bytes = int2Bytes(final_k_start_value);
        for (byte b : final_k_start_value_bytes) cur_byte.add(b);
        int bit_width_final = getBitWith(final_k_end_value - final_k_start_value);
        byte[] bit_width_bytes = int2Bytes(bit_width_final);
        for (byte b : bit_width_bytes) cur_byte.add(b);

        int left_bit_width = getBitWith(final_k_start_value);
        int right_bit_width = getBitWith(final_right_max - final_k_end_value);
        bit_width_bytes = int2Bytes(left_bit_width);
        for (byte b : bit_width_bytes) cur_byte.add(b);
        bit_width_bytes = int2Bytes(right_bit_width);
        for (byte b : bit_width_bytes) cur_byte.add(b);


        if (final_alpha == 0) {
            for (int i : bitmap) {
                byte[] index_bytes = intByte2Bytes(i);
                for (byte b : index_bytes) cur_byte.add(b);
            }
            for (int i : bitmap_outlier) {
                byte[] index_bytes = intByte2Bytes(i);
                for (byte b : index_bytes) cur_byte.add(b);
            }
        } else {
            for (int i : final_left_outlier_index) {
                byte[] index_bytes = intByte2Bytes(i);
                for (byte b : index_bytes) cur_byte.add(b);
            }
            for (int i : final_right_outlier_index) {
                byte[] index_bytes = intByte2Bytes(i);
                for (byte b : index_bytes) cur_byte.add(b);
            }
        }


        cur_byte.addAll(encodeOutlier2Bytes(final_normal, bit_width_final));
        if (final_k_start_value != 0)
            cur_byte.addAll(encodeOutlier2Bytes(final_left_outlier, left_bit_width));
        if (final_right_max - final_k_end_value != 0)
            cur_byte.addAll(encodeOutlier2Bytes(final_right_outlier, right_bit_width));
//        }


        return cur_byte;
    }

    public static int ReorderingRegressionEncoder(
            ArrayList<Integer> data, int block_size, double x_c, int beta, int block_i) throws IOException {
        block_size++;
        int bits_number = 0;

        ArrayList<Integer> ts_block = new ArrayList<>();
        for (int j = 0; j < block_size; j++) {
            ts_block.add(data.get(j + block_i * block_size));
        }
        ArrayList<Integer> result2 = new ArrayList<>();

        splitTimeStamp3(ts_block, result2);

        bits_number += bosEncode(ts_block, x_c, beta, 0);


        return bits_number;
    }


    public static ArrayList<ArrayList<Integer>> ReorderingRegressionDecoder(ArrayList<Byte> encoded) {
        ArrayList<ArrayList<Integer>> data = new ArrayList<>();
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

        for (int k = 0; k < block_num; k++) {
            ArrayList<Integer> time_list = new ArrayList<>();
            ArrayList<Integer> value_list = new ArrayList<>();

            ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

            int time0 = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;
            int value0 = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;

            float theta0_r = bytes2float(encoded, decode_pos);
            decode_pos += 4;
            float theta1_r = bytes2float(encoded, decode_pos);
            decode_pos += 4;
            float theta0_v = bytes2float(encoded, decode_pos);
            decode_pos += 4;
            float theta1_v = bytes2float(encoded, decode_pos);
            decode_pos += 4;

            int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;
            time_list = decodebitPacking(encoded, decode_pos, max_bit_width_time, 0, block_size);
            decode_pos += max_bit_width_time * (block_size - 1) / 8;

            int max_bit_width_value = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;
            value_list = decodebitPacking(encoded, decode_pos, max_bit_width_value, 0, block_size);
            decode_pos += max_bit_width_value * (block_size - 1) / 8;

            int td_common = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;

            int ti_pre = time0;
            int vi_pre = value0;
            for (int i = 0; i < block_size - 1; i++) {
                int ti = (int) ((double) theta1_r * ti_pre + (double) theta0_r + time_list.get(i));
                time_list.set(i, ti);
                ti_pre = ti;

                int vi = (int) ((double) theta1_v * vi_pre + (double) theta0_v + value_list.get(i));
                value_list.set(i, vi);
                vi_pre = vi;
            }

            ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
            ts_block_tmp0.add(time0);
            ts_block_tmp0.add(value0);
            ts_block.add(ts_block_tmp0);
            for (int i = 0; i < block_size - 1; i++) {
                int ti = (time_list.get(i) - time0) * td_common + time0;
                ArrayList<Integer> ts_block_tmp = new ArrayList<>();
                ts_block_tmp.add(ti);
                ts_block_tmp.add(value_list.get(i));
                ts_block.add(ts_block_tmp);
            }
//            quickSort(ts_block, 0, 0, block_size - 1);
            data.addAll(ts_block);
        }

        if (remain_length == 1) {
            int timestamp_end = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;
            int value_end = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;
            ArrayList<Integer> ts_block_end = new ArrayList<>();
            ts_block_end.add(timestamp_end);
            ts_block_end.add(value_end);
            data.add(ts_block_end);
        }
        if (remain_length != 0 && remain_length != 1) {
            ArrayList<Integer> time_list = new ArrayList<>();
            ArrayList<Integer> value_list = new ArrayList<>();

            ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

            int time0 = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;
            int value0 = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;

            float theta0_r = bytes2float(encoded, decode_pos);
            decode_pos += 4;
            float theta1_r = bytes2float(encoded, decode_pos);
            decode_pos += 4;
            float theta0_v = bytes2float(encoded, decode_pos);
            decode_pos += 4;
            float theta1_v = bytes2float(encoded, decode_pos);
            decode_pos += 4;

            int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;
            time_list =
                    decodebitPacking(encoded, decode_pos, max_bit_width_time, 0, remain_length + zero_number);
            decode_pos += max_bit_width_time * (remain_length + zero_number - 1) / 8;

            int max_bit_width_value = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;
            value_list =
                    decodebitPacking(
                            encoded, decode_pos, max_bit_width_value, 0, remain_length + zero_number);
            decode_pos += max_bit_width_value * (remain_length + zero_number - 1) / 8;

            int td_common = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;

            int ti_pre = time0;
            int vi_pre = value0;
            for (int i = 0; i < remain_length + zero_number - 1; i++) {
                int ti = (int) ((double) theta1_r * ti_pre + (double) theta0_r + time_list.get(i));
                time_list.set(i, ti);
                ti_pre = ti;

                int vi = (int) ((double) theta1_v * vi_pre + (double) theta0_v + value_list.get(i));
                value_list.set(i, vi);
                vi_pre = vi;
            }

            ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
            ts_block_tmp0.add(time0);
            ts_block_tmp0.add(value0);
            ts_block.add(ts_block_tmp0);
            for (int i = 0; i < remain_length + zero_number - 1; i++) {
                int ti = (time_list.get(i) - time0) * td_common + time0;
                ArrayList<Integer> ts_block_tmp = new ArrayList<>();
                ts_block_tmp.add(ti);
                ts_block_tmp.add(value_list.get(i));
                ts_block.add(ts_block_tmp);
            }

//            quickSort(ts_block, 0, 0, remain_length + zero_number - 1);

            for (int i = zero_number; i < remain_length + zero_number; i++) {
                data.add(ts_block.get(i));
            }
        }
        return data;
    }

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
        String parent_dir = "C:\\Users\\Jinnsjao Shawl\\Documents\\GitHub\\encoding-outlier\\";
        String output_parent_dir = parent_dir + "vldb\\compression_ratio\\best_spread";
        String input_parent_dir = parent_dir + "iotdb_test_small\\";
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
            dataset_block_size.add(1024);
        }

        output_path_list.add(output_parent_dir + "\\CS-Sensors_ratio.csv"); // 0
//        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "\\Metro-Traffic_ratio.csv");// 1
//        dataset_block_size.add(512);
        output_path_list.add(output_parent_dir + "\\USGS-Earthquakes_ratio.csv");// 2
//        dataset_block_size.add(512);
        output_path_list.add(output_parent_dir + "\\YZ-Electricity_ratio.csv"); // 3
//        dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "\\GW-Magnetic_ratio.csv"); //4
//        dataset_block_size.add(128);
        output_path_list.add(output_parent_dir + "\\TY-Fuel_ratio.csv");//5
//        dataset_block_size.add(64);
        output_path_list.add(output_parent_dir + "\\Cyber-Vehicle_ratio.csv"); //6
//        dataset_block_size.add(128);
        output_path_list.add(output_parent_dir + "\\Vehicle-Charge_ratio.csv");//7
//        dataset_block_size.add(512);
        output_path_list.add(output_parent_dir + "\\Nifty-Stocks_ratio.csv");//8
//        dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "\\TH-Climate_ratio.csv");//9
//        dataset_block_size.add(512);
        output_path_list.add(output_parent_dir + "\\TY-Transport_ratio.csv");//10
//        dataset_block_size.add(512);
        output_path_list.add(output_parent_dir + "\\EPM-Education_ratio.csv");//11
//        dataset_block_size.add(512);


        ArrayList<Integer> columnIndexes = new ArrayList<>(); // set the column indexes of compressed
        for (int i = 0; i < 2; i++) {
            columnIndexes.add(i, i);
        }

//        for (int file_i = 11; file_i < 12; file_i++) {
        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

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
                    "Points",
                    "Cost",
                    "x_c",
                    "beta"
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
                ArrayList<ArrayList<Integer>> data_decoded = new ArrayList<>();


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
                int block_num = data1.size() / 1025;
                for (int block_i = 0; block_i < block_num; block_i++) {

                    double cost = Double.MAX_VALUE;
                    double best_x_c = 0;
                    int best_beta = 1;
                    for (double x_c = 0; x_c < 1.009; x_c += 0.01) {
                        for (int beta = 1; beta < 15; beta++) {

                            int repeatTime2 = 1;

                            ArrayList<Byte> buffer2 = new ArrayList<>();
                            long buffer_bits = 0;
                            for (int repeat = 0; repeat < repeatTime2; repeat++) {
//                            buffer1 = ReorderingRegressionEncoder(data1, dataset_block_size.get(file_i), k);
                                buffer_bits = ReorderingRegressionEncoder(data2, dataset_block_size.get(file_i), x_c, beta, block_i);
                            }

                            if (cost > buffer_bits) {
                                best_x_c = x_c;
                                best_beta = beta;
                                cost = buffer_bits;
                            }


                        }
                    }
                    String[] record = {
                            f.toString(),
                            "Outlier",
                            String.valueOf(data1.size()),
                            String.valueOf(cost),
                            String.valueOf(best_x_c),
                            String.valueOf(best_beta),
                    };
                    writer.writeRecord(record);
                }
            }

            writer.close();

        }
    }

}
