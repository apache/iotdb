package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Stack;

import static java.lang.Math.abs;

public class RegerBlockSizeWithoutReordering {
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

    public static byte[] bitWidth2Bytes(int integer) {
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

    public static int bytes2BitWidth(ArrayList<Byte> encoded, int start, int num) {
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

    public static byte[] bitPacking(ArrayList<Integer> numbers, int bit_width) {
        int block_num = numbers.size() / 8;
        byte[] result = new byte[bit_width * block_num];
        for (int i = 0; i < block_num; i++) {
            for (int j = 0; j < bit_width; j++) {
                int tmp_int = 0;
                for (int k = 0; k < 8; k++) {
                    tmp_int += (((numbers.get(i * 8 + k) >> j) % 2) << k);
                }
                result[i * bit_width + j] = (byte) tmp_int;
            }
        }
        return result;
    }

    public static byte[] bitPacking(ArrayList<ArrayList<Integer>> numbers, int index, int bit_width) {
        int block_num = numbers.size() / 8;
        byte[] result = new byte[bit_width * block_num];
        for (int i = 0; i < block_num; i++) {
            for (int j = 0; j < bit_width; j++) {
                int tmp_int = 0;
                for (int k = 0; k < 8; k++) {
                    tmp_int += (((numbers.get(i * 8 + k + 1).get(index) >> j) % 2) << k);
                }
                result[i * bit_width + j] = (byte) tmp_int;
            }
        }
        return result;
    }

    public static byte[] bitPacking(ArrayList<ArrayList<Integer>> numbers, int index, int start, int block_num, int bit_width) {
        block_num = block_num / 8;
        byte[] result = new byte[bit_width * block_num];

        for (int i = 0; i < block_num; i++) {
            for (int j = 0; j < bit_width; j++) {
                int tmp_int = 0;
                for (int k = 0; k < 8; k++) {
                    tmp_int += (((numbers.get(start + i * 8 + k).get(index) >> j) % 2) << k);
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

    public static int part(ArrayList<ArrayList<Integer>> arr, int index, int low, int high) {
        ArrayList<Integer> tmp = arr.get(low);
        while (low < high) {
            while (low < high
                    && (arr.get(high).get(index) > tmp.get(index)
                    || (Objects.equals(arr.get(high).get(index), tmp.get(index))
                    && arr.get(high).get(index ^ 1) >= tmp.get(index ^ 1)))) {
                high--;
            }
            arr.set(low, arr.get(high));
            while (low < high
                    && (arr.get(low).get(index) < tmp.get(index)
                    || (Objects.equals(arr.get(low).get(index), tmp.get(index))
                    && arr.get(low).get(index ^ 1) <= tmp.get(index ^ 1)))) {
                low++;
            }

            arr.set(high, arr.get(low));
        }
        arr.set(low, tmp);
        return low;
    }

    public static void quickSort(ArrayList<ArrayList<Integer>> arr, int index, int low, int high) {
        Stack<Integer> stack = new Stack<>();
        int mid = part(arr, index, low, high);
        if (mid + 1 < high) {
            stack.push(mid + 1);
            stack.push(high);
        }
        if (mid - 1 > low) {
            stack.push(low);
            stack.push(mid - 1);
        }
        while (stack.empty() == false) {
            high = stack.pop();
            low = stack.pop();
            mid = part(arr, index, low, high);
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
            ArrayList<ArrayList<Integer>> ts_block, ArrayList<Integer> result) {
        int td_common = 0;
        for (int i = 1; i < ts_block.size(); i++) {
            int time_diffi = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
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

        int t0 = ts_block.get(0).get(0);
        for (int i = 0; i < ts_block.size(); i++) {
            ArrayList<Integer> tmp = new ArrayList<>();
            int interval_i = (ts_block.get(i).get(0) - t0) / td_common;
            tmp.add(t0 + interval_i);
            tmp.add(ts_block.get(i).get(1));
            ts_block.set(i, tmp);
        }
        result.add(td_common);
    }

    public static ArrayList<ArrayList<Integer>> getEncodeBitsRegression(
            ArrayList<ArrayList<Integer>> ts_block,
            int block_size,
            ArrayList<Integer> result,
            ArrayList<Float> theta) {
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
        theta.clear();

        long sum_X_r = 0;
        long sum_Y_r = 0;
        long sum_squ_X_r = 0;
        long sum_squ_XY_r = 0;
        long sum_X_v = 0;
        long sum_Y_v = 0;
        long sum_squ_X_v = 0;
        long sum_squ_XY_v = 0;

        for (int i = 1; i < block_size; i++) {
            sum_X_r += (ts_block.get(i - 1).get(0));
            sum_X_v += ts_block.get(i - 1).get(1);
            sum_Y_r += (ts_block.get(i).get(0));
            sum_Y_v += ts_block.get(i).get(1);
            sum_squ_X_r += ((long) (ts_block.get(i - 1).get(0)) * (ts_block.get(i - 1).get(0)));
            sum_squ_X_v += ((long) ts_block.get(i - 1).get(1) * ts_block.get(i - 1).get(1));
            sum_squ_XY_r += ((long) (ts_block.get(i - 1).get(0)) * (ts_block.get(i).get(0)));
            sum_squ_XY_v += ((long) ts_block.get(i - 1).get(1) * ts_block.get(i).get(1));
        }

        int m_reg = block_size - 1;
        float theta0_r = 0.0F;
        float theta1_r = 1.0F;
        if (m_reg * sum_squ_X_r != sum_X_r * sum_X_r) {
            theta0_r =
                    (float) (sum_squ_X_r * sum_Y_r - sum_X_r * sum_squ_XY_r)
                            / (float) (m_reg * sum_squ_X_r - sum_X_r * sum_X_r);
            theta1_r =
                    (float) (m_reg * sum_squ_XY_r - sum_X_r * sum_Y_r)
                            / (float) (m_reg * sum_squ_X_r - sum_X_r * sum_X_r);
        }

        float theta0_v = 0.0F;
        float theta1_v = 1.0F;
        if (m_reg * sum_squ_X_v != sum_X_v * sum_X_v) {
            theta0_v =
                    (float) (sum_squ_X_v * sum_Y_v - sum_X_v * sum_squ_XY_v)
                            / (float) (m_reg * sum_squ_X_v - sum_X_v * sum_X_v);
            theta1_v =
                    (float) (m_reg * sum_squ_XY_v - sum_X_v * sum_Y_v)
                            / (float) (m_reg * sum_squ_X_v - sum_X_v * sum_X_v);
        }


        ArrayList<Integer> tmp0 = new ArrayList<>();
        tmp0.add(ts_block.get(0).get(0));
        tmp0.add(ts_block.get(0).get(1));
        ts_block_delta.add(tmp0);

        // delta to Regression
        for (int j = 1; j < block_size; j++) {
            int epsilon_r =
                    (ts_block.get(j).get(0)
                            - (int) (theta0_r + theta1_r * (float) ts_block.get(j - 1).get(0)));
            int epsilon_v =
                    (ts_block.get(j).get(1)  - (int) ( theta0_v +  theta1_v * (float) ts_block.get(j - 1).get(1)));

            if (epsilon_r < timestamp_delta_min) {
                timestamp_delta_min = epsilon_r;
            }
            if (epsilon_v < value_delta_min) {
                value_delta_min = epsilon_v;
            }
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add(epsilon_r);
            tmp.add(epsilon_v);
            ts_block_delta.add(tmp);



        }


        int max_interval = Integer.MIN_VALUE;
        int max_value = Integer.MIN_VALUE;
        int length = 0;
        for (int j = block_size - 1; j > 0; j--) {

            int epsilon_r =ts_block_delta.get(j).get(0) - timestamp_delta_min;
            int epsilon_v =ts_block_delta.get(j).get(1) - value_delta_min;


            length += getBitWith(epsilon_r);
            length += getBitWith(epsilon_v);
            if (epsilon_r > max_interval) {
                max_interval = epsilon_r;
            }
            if (epsilon_v > max_value) {
                max_value = epsilon_v;
            }
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add(epsilon_r);
            tmp.add(epsilon_v);
            ts_block_delta.set(j, tmp);



        }

        int max_bit_width_interval = getBitWith(max_interval);
        int max_bit_width_value = getBitWith(max_value);


        result.clear();

        result.add(length);
        result.add(max_bit_width_interval);
        result.add(max_bit_width_value);

        result.add(timestamp_delta_min);
        result.add(value_delta_min);

        theta.add(theta0_r);
        theta.add(theta1_r);
        theta.add(theta0_v);
        theta.add(theta1_v);

        return ts_block_delta;
    }
    public static ArrayList<Byte> encode2Bytes(
            ArrayList<ArrayList<Integer>> ts_block,
            ArrayList<Integer> raw_length,
            ArrayList<Float> theta,
            ArrayList<Integer> result2) {
        ArrayList<Byte> encoded_result = new ArrayList<>();


        // encode interval0 and value0
        byte[] interval0_byte = int2Bytes(ts_block.get(0).get(0));
        for (byte b : interval0_byte) encoded_result.add(b);
        byte[] value0_byte = int2Bytes(ts_block.get(0).get(1));
        for (byte b : value0_byte) encoded_result.add(b);

        // encode theta
        byte[] theta0_r_byte = float2bytes(theta.get(0) + raw_length.get(3));
        for (byte b : theta0_r_byte) encoded_result.add(b);
        byte[] theta1_r_byte = float2bytes(theta.get(1));
        for (byte b : theta1_r_byte) encoded_result.add(b);
        byte[] theta0_v_byte = float2bytes(theta.get(2) + raw_length.get(4));
        for (byte b : theta0_v_byte) encoded_result.add(b);
        byte[] theta1_v_byte = float2bytes(theta.get(3));
        for (byte b : theta1_v_byte) encoded_result.add(b);

        // encode interval
        byte[] max_bit_width_interval_byte = int2Bytes(raw_length.get(1));
        for (byte b : max_bit_width_interval_byte) encoded_result.add(b);
        byte[] timestamp_bytes = bitPacking(ts_block, 0, raw_length.get(1));
        for (byte b : timestamp_bytes) encoded_result.add(b);

        // encode value
        byte[] max_bit_width_value_byte = int2Bytes(raw_length.get(2));
        for (byte b : max_bit_width_value_byte) encoded_result.add(b);
        byte[] value_bytes = bitPacking(ts_block, 1, raw_length.get(2));
        for (byte b : value_bytes) encoded_result.add(b);

        byte[] td_common_byte = int2Bytes(result2.get(0));
        for (byte b : td_common_byte) encoded_result.add(b);

        return encoded_result;
    }

    public static ArrayList<Byte> encodeRLEBitWidth2Bytes(
            ArrayList<ArrayList<Integer>> bit_width_segments) {
        ArrayList<Byte> encoded_result = new ArrayList<>();

        ArrayList<ArrayList<Integer>> run_length_time = new ArrayList<>();
        ArrayList<ArrayList<Integer>> run_length_value = new ArrayList<>();

        int count_of_time = 0;
        int count_of_value = 0;
        int pre_time = bit_width_segments.get(0).get(0);
        int pre_value = bit_width_segments.get(0).get(1);
        int size = bit_width_segments.size();
        for (int i = 1; i < size; i++) {
            int cur_time = bit_width_segments.get(i).get(0);
            int cur_value = bit_width_segments.get(i).get(1);
            if (cur_time != pre_time) {
                ArrayList<Integer> tmp = new ArrayList<>();
                tmp.add(count_of_time);
                tmp.add(pre_time);
                run_length_time.add(tmp);
                pre_time = cur_time;
                count_of_time = 0;
            } else {
                count_of_time++;
                if (count_of_time == 256) {
                    ArrayList<Integer> tmp = new ArrayList<>();
                    tmp.add(count_of_time);
                    tmp.add(pre_time);
                    run_length_time.add(tmp);
                    count_of_time = 0;
                }
            }

            if (cur_value != pre_value) {
                ArrayList<Integer> tmp = new ArrayList<>();
                tmp.add(count_of_value);
                tmp.add(pre_value);
                run_length_value.add(tmp);
                pre_value = cur_value;
                count_of_value = 0;
            } else {
                count_of_value++;
                if (count_of_value == 256) {
                    ArrayList<Integer> tmp = new ArrayList<>();
                    tmp.add(count_of_value);
                    tmp.add(pre_value);
                    run_length_value.add(tmp);
                    count_of_value = 0;
                }
            }

        }
        if (count_of_time != 0) {
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add(count_of_time);
            tmp.add(pre_time);
            run_length_time.add(tmp);
        }
        if (count_of_value != 0) {
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add(count_of_value);
            tmp.add(pre_value);
            run_length_value.add(tmp);
        }

        for (ArrayList<Integer> bit_width_time : run_length_time) {
            byte[] timestamp_bytes = bitWidth2Bytes(bit_width_time.get(0));
            for (byte b : timestamp_bytes) encoded_result.add(b);
            byte[] value_bytes = bitWidth2Bytes(bit_width_time.get(1));
            for (byte b : value_bytes) encoded_result.add(b);
        }
        for (ArrayList<Integer> bit_width_value : run_length_value) {
            byte[] timestamp_bytes = bitWidth2Bytes(bit_width_value.get(0));
            for (byte b : timestamp_bytes) encoded_result.add(b);
            byte[] value_bytes = bitWidth2Bytes(bit_width_value.get(1));
            for (byte b : value_bytes) encoded_result.add(b);
        }
        return encoded_result;
    }

    public static ArrayList<Byte> ReorderingRegressionEncoder(
            ArrayList<ArrayList<Integer>> data, int block_size, int[] third_value, int segment_size) {
        block_size++;
        ArrayList<Byte> encoded_result = new ArrayList<Byte>();
        int length_all = data.size();
        byte[] length_all_bytes = int2Bytes(length_all);
        for (byte b : length_all_bytes) encoded_result.add(b);
        int block_num = length_all / block_size;

        // encode block size (Integer)
        byte[] block_size_byte = int2Bytes(block_size);
        for (byte b : block_size_byte) encoded_result.add(b);


        for (int i = 0; i < block_num; i++) {
            ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
            for (int j = 0; j < block_size; j++) {
                ts_block.add(data.get(j + i * block_size));
            }

            ArrayList<Integer> result2 = new ArrayList<>();
            //      result2.add(1);
            splitTimeStamp3(ts_block, result2);
            quickSort(ts_block, 0, 0, block_size - 1);
            ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
            ArrayList<Float> theta = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegression(ts_block, block_size, raw_length, theta);
            ArrayList<ArrayList<Integer>> bit_width_segments = new ArrayList<>();
            int segment_n = (block_size - 1) / segment_size;
            for (int segment_i = 0; segment_i < segment_n; segment_i++) {
                int bit_width_time = Integer.MIN_VALUE;
                int bit_width_value = Integer.MIN_VALUE;

                for (int data_i = segment_i * segment_size + 1; data_i < (segment_i + 1) * segment_size + 1; data_i++) {
                    int cur_bit_width_time = getBitWith(ts_block_delta.get(data_i).get(0));
                    int cur_bit_width_value = getBitWith(ts_block_delta.get(data_i).get(1));
                    if (cur_bit_width_time > bit_width_time) {
                        bit_width_time = cur_bit_width_time;
                    }
                    if (cur_bit_width_value > bit_width_value) {
                        bit_width_value = cur_bit_width_value;
                    }
                }
                ArrayList<Integer> bit_width = new ArrayList<>();
                bit_width.add(bit_width_time);
                bit_width.add(bit_width_value);
                bit_width_segments.add(bit_width);
            }


            ArrayList<Byte> cur_encoded_result = encodeSegment2Bytes(ts_block_delta, bit_width_segments, raw_length, segment_size, theta, result2);
            encoded_result.addAll(cur_encoded_result);

        }


        int remaining_length = length_all - block_num * block_size;
        if (remaining_length == 1) {
            byte[] timestamp_end_bytes = int2Bytes(data.get(data.size() - 1).get(0));
            for (byte b : timestamp_end_bytes) encoded_result.add(b);
            byte[] value_end_bytes = int2Bytes(data.get(data.size() - 1).get(1));
            for (byte b : value_end_bytes) encoded_result.add(b);
        }
        if (remaining_length != 0 && remaining_length != 1) {
            ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();

            int min_time = data.get(block_num * block_size).get(0);
            for (int j = block_num * block_size; j < length_all; j++) {
                data.get(j).set(0,data.get(j).get(0) - min_time);
                ts_block.add(data.get(j));
                ts_block_reorder.add(data.get(j));
            }
            ArrayList<Integer> result2 = new ArrayList<>();
            splitTimeStamp3(ts_block, result2);

            quickSort(ts_block, 0, 0, remaining_length - 1);
            int supple_length;
            if (remaining_length % segment_size == 0) {
                supple_length = 1;
            } else if (remaining_length % segment_size == 1) {
                supple_length = 0;
            } else {
                supple_length = segment_size + 1 - remaining_length % segment_size;
            }
            // time-order
            ArrayList<Integer> raw_length =
                    new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
            ArrayList<Float> theta = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta =  getEncodeBitsRegression(ts_block, remaining_length, raw_length, theta);

            for (int s = 0; s < supple_length; s++) {
                ArrayList<Integer> tmp = new ArrayList<>();
                tmp.add(0);
                tmp.add(0);
                ts_block_delta.add(tmp);
            }
            ArrayList<ArrayList<Integer>> bit_width_segments_time = segmentBitPacking(ts_block_delta, remaining_length+supple_length, segment_size);

            ArrayList<Byte> cur_encoded_result = encodeSegment2Bytes(ts_block_delta, bit_width_segments_time, raw_length, segment_size, theta, result2);

            encoded_result.addAll(cur_encoded_result);
        }

        return encoded_result;
    }
    public static ArrayList<ArrayList<Integer>> segmentBitPacking(ArrayList<ArrayList<Integer>> ts_block_delta, int block_size, int segment_size) {
        ArrayList<ArrayList<Integer>> bit_width_segments = new ArrayList<>();
        int segment_n = (block_size - 1) / segment_size;
        for (int segment_i = 0; segment_i < segment_n; segment_i++) {
            int bit_width_time = Integer.MIN_VALUE;
            int bit_width_value = Integer.MIN_VALUE;

            for (int data_i = segment_i * segment_size + 1; data_i < (segment_i + 1) * segment_size + 1; data_i++) {
                int cur_bit_width_time = getBitWith(ts_block_delta.get(data_i).get(0));
                int cur_bit_width_value = getBitWith(ts_block_delta.get(data_i).get(1));
                if (cur_bit_width_time > bit_width_time) {
                    bit_width_time = cur_bit_width_time;
                }
                if (cur_bit_width_value > bit_width_value) {
                    bit_width_value = cur_bit_width_value;
                }
            }
            ArrayList<Integer> bit_width = new ArrayList<>();
            bit_width.add(bit_width_time);
            bit_width.add(bit_width_value);
            bit_width_segments.add(bit_width);
        }

        return bit_width_segments;
    }
    private static ArrayList<Byte> encodeSegment2Bytes(ArrayList<ArrayList<Integer>> delta_segments, ArrayList<ArrayList<Integer>> bit_width_segments, ArrayList<Integer> raw_length, int segment_size, ArrayList<Float> theta, ArrayList<Integer> result2) {
        ArrayList<Byte> encoded_result = new ArrayList<>();
        int block_size = delta_segments.size();
        int segment_n = block_size / segment_size;

        // encode interval0 and value0
        byte[] interval0_byte = int2Bytes(delta_segments.get(0).get(0));
        for (byte b : interval0_byte) encoded_result.add(b);
        byte[] value0_byte = int2Bytes(delta_segments.get(0).get(1));
        for (byte b : value0_byte) encoded_result.add(b);

        // encode theta
        byte[] theta0_r_byte = float2bytes(theta.get(0) + raw_length.get(3));
        for (byte b : theta0_r_byte) encoded_result.add(b);
        byte[] theta1_r_byte = float2bytes(theta.get(1));
        for (byte b : theta1_r_byte) encoded_result.add(b);
        byte[] theta0_v_byte = float2bytes(theta.get(2) + raw_length.get(4));
        for (byte b : theta0_v_byte) encoded_result.add(b);
        byte[] theta1_v_byte = float2bytes(theta.get(3));
        for (byte b : theta1_v_byte) encoded_result.add(b);
        encoded_result.addAll(encodeRLEBitWidth2Bytes(bit_width_segments));
        for (int segment_i = 0; segment_i < segment_n; segment_i++) {
            int bit_width_time = bit_width_segments.get(segment_i).get(0);
            int bit_width_value = bit_width_segments.get(segment_i).get(1);
            byte[] timestamp_bytes = bitPacking(delta_segments, 0, segment_i * segment_size + 1, segment_size, bit_width_time);
            for (byte b : timestamp_bytes) encoded_result.add(b);
            byte[] value_bytes = bitPacking(delta_segments, 1, segment_i * segment_size + 1, segment_size, bit_width_value);
            for (byte b : value_bytes) encoded_result.add(b);
        }

        byte[] td_common_byte = int2Bytes(result2.get(0));
        for (byte b : td_common_byte) encoded_result.add(b);

        return encoded_result;
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


            // get bit_width_segments
            ArrayList<Integer> bit_width_segments_time = new ArrayList<>();
            ArrayList<Integer> bit_width_segments_value = new ArrayList<>();
            int sum_time_bit_width = 0;
            int sum_value_bit_width = 0;
            int segment_n = (block_size - 1) / 8;
            while (sum_time_bit_width != segment_n) {
                int max_bit_width_time_nums = bytes2BitWidth(encoded, decode_pos, 1);
                decode_pos += 1;
                int max_bit_width_time = bytes2BitWidth(encoded, decode_pos, 1);
                decode_pos += 1;
                sum_time_bit_width += max_bit_width_time_nums;
                for (int i = 0; i < max_bit_width_time_nums; i++) {
                    bit_width_segments_time.add(max_bit_width_time);
                }
            }
            while (sum_value_bit_width != segment_n) {
                int max_bit_width_value_nums = bytes2BitWidth(encoded, decode_pos, 1);
                decode_pos += 1;
                int max_bit_width_value = bytes2BitWidth(encoded, decode_pos, 1);
                decode_pos += 1;
                sum_value_bit_width += max_bit_width_value_nums;
                for (int i = 0; i < max_bit_width_value_nums; i++) {
                    bit_width_segments_value.add(max_bit_width_value);
                }
            }

//      int max_bit_width_time = bytes2Integer(encoded, decode_pos, 4);
//      decode_pos += 4;
            for (int i = 0; i < segment_n; i++) {
                int max_bit_width_time = bit_width_segments_time.get(i);
                ArrayList<Integer> tmp_time_list = decodebitPacking(encoded, decode_pos, max_bit_width_time, 0, 9);
                decode_pos += max_bit_width_time * 8;
                time_list.addAll(tmp_time_list);
                int max_bit_width_value = bit_width_segments_value.get(i);
                ArrayList<Integer> tmp_value_list = decodebitPacking(encoded, decode_pos, max_bit_width_value, 0, 9);
                decode_pos += max_bit_width_value * 8;
                value_list.addAll(tmp_value_list);
            }



            int td_common = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;

            int ti_pre = time0;
            int vi_pre = value0;
            for (int i = 0; i < block_size - 1; i++) {
                int ti = (int) ((double) theta0_r + (double) theta1_r * (double) ti_pre) + time_list.get(i);
                time_list.set(i, ti);
                ti_pre = ti;

                int vi =
                        (int) ((double) theta0_v + (double) theta1_v * (double) vi_pre) + value_list.get(i);
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
            quickSort(ts_block, 0, 0, block_size - 1);
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
            for (int i = 0; i < remain_length - 1; i++) {
                int ti = (int) ((double) theta0_r + (double) theta1_r * (double) ti_pre) + time_list.get(i);
                time_list.set(i, ti);
                ti_pre = ti;
                int vi =
                        (int) ((double) theta0_v + (double) theta1_v * (double) vi_pre) + value_list.get(i);
                value_list.set(i, vi);
                vi_pre = vi;
            }

            ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
            ts_block_tmp0.add(time0);
            ts_block_tmp0.add(value0);
            ts_block.add(ts_block_tmp0);
            for (int i = 0; i < remain_length - 1; i++) {
                int ti = (time_list.get(i) - time0) * td_common + time0;
                ArrayList<Integer> ts_block_tmp = new ArrayList<>();
                ts_block_tmp.add(ti);
                ts_block_tmp.add(value_list.get(i));
                ts_block.add(ts_block_tmp);
            }

            quickSort(ts_block, 0, 0, remain_length - 1);
            for (int i = 0; i < remain_length; i++) {
                data.add(ts_block.get(i));
            }
        }
        return data;
    }

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {

        String parent_dir = "C:\\Users\\Jinnsjao Shawl\\Documents\\GitHub\\encoding-reorder\\vldb\\compression_ratio\\block_size_only_segment";
        String input_parent_dir = "C:\\Users\\Jinnsjao Shawl\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        ArrayList<int[]> dataset_third = new ArrayList<>();

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

        int[] dataset_0 = {547, 2816};
        int[] dataset_1 = {1719, 3731};
        int[] dataset_2 = {-48, -11, 6, 25, 52};
        int[] dataset_3 = {8681, 13584};
        int[] dataset_4 = {79, 184, 274};
        int[] dataset_5 = {17, 68};
        int[] dataset_6 = {677};
        int[] dataset_7 = {1047, 1725};
        int[] dataset_8 = {227, 499, 614, 1013};
        int[] dataset_9 = {474, 678};
        int[] dataset_10 = {4, 30, 38, 49, 58};
        int[] dataset_11 = {5182, 8206};

        dataset_third.add(dataset_0);
        dataset_third.add(dataset_1);
        dataset_third.add(dataset_2);
        dataset_third.add(dataset_3);
        dataset_third.add(dataset_4);
        dataset_third.add(dataset_5);
        dataset_third.add(dataset_6);
        dataset_third.add(dataset_7);
        dataset_third.add(dataset_8);
        dataset_third.add(dataset_9);
        dataset_third.add(dataset_10);
        dataset_third.add(dataset_11);

        for (int i = 0; i < dataset_name.size(); i++) {
            input_path_list.add(input_parent_dir + dataset_name.get(i));
        }

        output_path_list.add(parent_dir + "\\CS-Sensors_ratio.csv"); // 0
        dataset_block_size.add(1024);
        output_path_list.add(parent_dir + "\\Metro-Traffic_ratio.csv");// 1
        dataset_block_size.add(512);
        output_path_list.add(parent_dir + "\\USGS-Earthquakes_ratio.csv");// 2
        dataset_block_size.add(512);
        output_path_list.add(parent_dir + "\\YZ-Electricity_ratio.csv"); // 3
        dataset_block_size.add(1024);
        output_path_list.add(parent_dir + "\\GW-Magnetic_ratio.csv"); //4
        dataset_block_size.add(128);
        output_path_list.add(parent_dir + "\\TY-Fuel_ratio.csv");//5
        dataset_block_size.add(64);
        output_path_list.add(parent_dir + "\\Cyber-Vehicle_ratio.csv"); //6
        dataset_block_size.add(128);
        output_path_list.add(parent_dir + "\\Vehicle-Charge_ratio.csv");//7
        dataset_block_size.add(512);
        output_path_list.add(parent_dir + "\\Nifty-Stocks_ratio.csv");//8
        dataset_block_size.add(256);
        output_path_list.add(parent_dir + "\\TH-Climate_ratio.csv");//9
        dataset_block_size.add(512);
        output_path_list.add(parent_dir + "\\TY-Transport_ratio.csv");//10
        dataset_block_size.add(512);
        output_path_list.add(parent_dir + "\\EPM-Education_ratio.csv");//11
        dataset_block_size.add(512);

        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {
            String inputPath = input_path_list.get(file_i);
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
                    "Block Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {

                for (int block_size_exp = 13; block_size_exp >= 4; block_size_exp--) {
                int block_size = (int) Math.pow(2, block_size_exp);


                    InputStream inputStream = Files.newInputStream(f.toPath());
                    CsvReader loader = new CsvReader(inputStream, StandardCharsets.UTF_8);
                    ArrayList<ArrayList<Integer>> data = new ArrayList<>();
                    ArrayList<ArrayList<Integer>> data_decoded = new ArrayList<>();

                    // add a column to "data"
                    loader.readHeaders();
                    data.clear();
                    while (loader.readRecord()) {
                        ArrayList<Integer> tmp = new ArrayList<>();
                        tmp.add(Integer.valueOf(loader.getValues()[0]));
                        tmp.add(Integer.valueOf(loader.getValues()[1]));

                        data.add(tmp);
                    }
                    inputStream.close();
                    long encodeTime = 0;
                    long decodeTime = 0;
                    double ratio = 0;
                    double compressed_size = 0;
                    int repeatTime2 = 1;
                    for (int i = 0; i < repeatTime; i++) {
                        long s = System.nanoTime();
                        ArrayList<Byte> buffer = new ArrayList<>();
                        for (int repeat = 0; repeat < repeatTime2; repeat++)
                            buffer = ReorderingRegressionEncoder(data, block_size, dataset_third.get(file_i),16);
                        long e = System.nanoTime();
                        encodeTime += ((e - s) / repeatTime2);
                        compressed_size += buffer.size();
                        double ratioTmp = (double) buffer.size() / (double) (data.size() * Integer.BYTES * 2);
                        ratio += ratioTmp;
                        s = System.nanoTime();
                        e = System.nanoTime();
                        decodeTime += ((e - s) / repeatTime2);
                    }

                    ratio /= repeatTime;
                    compressed_size /= repeatTime;
                    encodeTime /= repeatTime;
                    decodeTime /= repeatTime;

                    String[] record = {
                            f.toString(),
                            "REGER-Without-Reordering",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data.size()),
                            String.valueOf(compressed_size),
                            String.valueOf(block_size_exp),
                            String.valueOf(ratio)
                    };
                    System.out.println(ratio);
                    writer.writeRecord(record);

                }
            }
            writer.close();
        }
    }
}
