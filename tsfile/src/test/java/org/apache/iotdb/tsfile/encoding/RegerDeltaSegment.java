package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.math3.random.AbstractRandomGenerator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Stack;

import static java.lang.Math.abs;

public class RegerDeltaSegment {
    public static int getBitWith(int num) {
        return 32 - Integer.numberOfLeadingZeros(num);
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
    public static byte[] bitPacking(ArrayList<ArrayList<Integer>> numbers, int index, int start, int block_num, int bit_width) {
        block_num = block_num / 8;
        byte[] result = new byte[bit_width * block_num];
//        System.out.println(start);
//        System.out.println("block_num"+block_num);

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
            ArrayList<Integer> result) {
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

        ArrayList<Integer> tmp0 = new ArrayList<>();
        tmp0.add(ts_block.get(0).get(0));
        tmp0.add(ts_block.get(0).get(1));
        ts_block_delta.add(tmp0);

        // delta to Regression
        for (int j = 1; j < block_size; j++) {
            int epsilon_r = ts_block.get(j).get(0) - ts_block.get(j - 1).get(0);
            int epsilon_v = ts_block.get(j).get(1) - ts_block.get(j - 1).get(1);

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
        int max_interval_i = -1;
        int max_value = Integer.MIN_VALUE;
        int max_value_i = -1;
        for (int j = block_size - 1; j > 0; j--) {
            int epsilon_r = ts_block_delta.get(j).get(0) - timestamp_delta_min;
            int epsilon_v = ts_block_delta.get(j).get(1) - value_delta_min;
            if (epsilon_r > max_interval) {
                max_interval = epsilon_r;
                max_interval_i = j;
            }
            if (epsilon_v > max_value) {
                max_value = epsilon_v;
                max_value_i = j;
            }
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add(epsilon_r);
            tmp.add(epsilon_v);
            ts_block_delta.set(j, tmp);
        }
        int max_bit_width_interval = getBitWith(max_interval);
        int max_bit_width_value = getBitWith(max_value);

        // calculate error
        int length = (max_bit_width_interval + max_bit_width_value) * (block_size - 1);
        result.clear();

        result.add(length);
        result.add(max_bit_width_interval);
        result.add(max_bit_width_value);

        result.add(timestamp_delta_min);
        result.add(value_delta_min);

        return ts_block_delta;
    }

    public static int getJStar(
            ArrayList<ArrayList<Integer>> ts_block,
            int alpha,
            int block_size,
            ArrayList<Integer> raw_length,
            int index) {
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int raw_timestamp_delta_max = Integer.MIN_VALUE;
        int raw_value_delta_max = Integer.MIN_VALUE;
        int raw_timestamp_delta_max_index = -1;
        int raw_value_delta_max_index = -1;
        int raw_bit_width_timestamp = 0;
        int raw_bit_width_value = 0;

        ArrayList<Integer> j_star_list = new ArrayList<>(); // beta list of min b phi alpha to j
        ArrayList<Integer> max_index = new ArrayList<>();
        int j_star = -1;

        if (alpha == -1) {
            return j_star;
        }
        for (int i = 1; i < block_size; i++) {
            int delta_t_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
            int delta_v_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
            if (delta_t_i < timestamp_delta_min) {
                timestamp_delta_min = delta_t_i;
            }
            if (delta_v_i < value_delta_min) {
                value_delta_min = delta_v_i;
            }
            if (delta_t_i > raw_timestamp_delta_max) {
                raw_timestamp_delta_max = delta_t_i;
                raw_timestamp_delta_max_index = i;
            }
            if (delta_v_i > raw_value_delta_max) {
                raw_value_delta_max = delta_v_i;
                raw_value_delta_max_index = i;
            }
        }
        for (int i = 1; i < block_size; i++) {
            int delta_t_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
            int delta_v_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);

            if (i != alpha
                    && (delta_t_i == raw_timestamp_delta_max || delta_v_i == raw_value_delta_max)) {
                max_index.add(i);
            }
        }
        raw_bit_width_timestamp = getBitWith(raw_timestamp_delta_max - timestamp_delta_min);
        raw_bit_width_value = getBitWith(raw_value_delta_max - value_delta_min);
        // alpha == 1
        if (alpha == 0) {
            for (int j = 2; j < block_size; j++) {
                if (!max_index.contains(j) && !max_index.contains(alpha + 1)) continue;
                ArrayList<Integer> b = adjust0(ts_block, alpha, j);
                if ((b.get(0) + b.get(1)) < (raw_bit_width_timestamp + raw_bit_width_value)) {
                    raw_bit_width_timestamp = b.get(0);
                    raw_bit_width_value = b.get(1);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp + raw_bit_width_value)) {
                    j_star_list.add(j);
                }
            }
            ArrayList<Integer> b = adjust0n1(ts_block);
            if ((b.get(0) + b.get(1)) < (raw_bit_width_timestamp + raw_bit_width_value)) {
                raw_bit_width_timestamp = b.get(0);
                raw_bit_width_value = b.get(1);
                j_star_list.clear();
                j_star_list.add(block_size);
            } else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp + raw_bit_width_value)) {
                j_star_list.add(block_size);
            }

        } // alpha == n
        else if (alpha == block_size - 1) {
            for (int j = 1; j < block_size - 1; j++) {
                if (!max_index.contains(j) && !max_index.contains(alpha + 1)) continue;
                ArrayList<Integer> b = adjustn(ts_block, alpha, j);
                if ((b.get(0) + b.get(1)) < (raw_bit_width_timestamp + raw_bit_width_value)) {
                    raw_bit_width_timestamp = b.get(0);
                    raw_bit_width_value = b.get(1);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp + raw_bit_width_value)) {
                    j_star_list.add(j);
                }
            }
            ArrayList<Integer> b = adjustn0(ts_block);
            if ((b.get(0) + b.get(1)) < (raw_bit_width_timestamp + raw_bit_width_value)) {
                raw_bit_width_timestamp = b.get(0);
                raw_bit_width_value = b.get(1);
                j_star_list.clear();
                j_star_list.add(0);
            } else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp + raw_bit_width_value)) {
                j_star_list.add(0);
            }
        } // alpha != 1 and alpha != n
        else {
            for (int j = 1; j < block_size; j++) {
                if (!max_index.contains(j) && !max_index.contains(alpha + 1)) continue;
                if (alpha != j && (alpha + 1) != j) {
                    ArrayList<Integer> b = adjustAlphaToJ(ts_block, alpha, j);
                    if ((b.get(0) + b.get(1)) < (raw_bit_width_timestamp + raw_bit_width_value)) {
                        raw_bit_width_timestamp = b.get(0);
                        raw_bit_width_value = b.get(1);
                        j_star_list.clear();
                        j_star_list.add(j);
                    } else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp + raw_bit_width_value)) {
                        j_star_list.add(j);
                    }
                }
            }
            ArrayList<Integer> b = adjustTo0(ts_block, alpha);
            if ((b.get(0) + b.get(1)) < (raw_bit_width_timestamp + raw_bit_width_value)) {
                raw_bit_width_timestamp = b.get(0);
                raw_bit_width_value = b.get(1);
                j_star_list.clear();
                j_star_list.add(0);
            } else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp + raw_bit_width_value)) {
                j_star_list.add(0);
            }
            b = adjustTon(ts_block, alpha);
            if ((b.get(0) + b.get(1)) < (raw_bit_width_timestamp + raw_bit_width_value)) {
                raw_bit_width_timestamp = b.get(0);
                raw_bit_width_value = b.get(1);
                j_star_list.clear();
                j_star_list.add(block_size);
            } else if ((b.get(0) + b.get(1)) == (raw_bit_width_timestamp + raw_bit_width_value)) {
                j_star_list.add(block_size);
            }
        }
        if (j_star_list.size() == 0) {
        } else {
            j_star = getIstarClose(alpha, j_star_list);
        }
        return j_star;
    }

    private static ArrayList<Integer> adjustTo0(ArrayList<ArrayList<Integer>> ts_block, int alpha) {
        int block_size = ts_block.size();
        assert alpha != block_size - 1;
        assert alpha != 0;
        ArrayList<Integer> b = new ArrayList<>();
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;

        for (int i = 1; i < block_size; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            if (i == (alpha + 1)) {
                timestamp_delta_i = ts_block.get(alpha + 1).get(0) - ts_block.get(alpha - 1).get(0);
                value_delta_i = ts_block.get(alpha + 1).get(1) - ts_block.get(alpha - 1).get(1);
            } else if (i == alpha) {
                timestamp_delta_i = ts_block.get(0).get(0) - ts_block.get(alpha).get(0);
                value_delta_i = ts_block.get(0).get(1) - ts_block.get(alpha).get(1);
            } else {
                timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
                value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
            }
            if (timestamp_delta_i > timestamp_delta_max) {
                timestamp_delta_max = timestamp_delta_i;
            }
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i > value_delta_max) {
                value_delta_max = value_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }
        }
        b.add(getBitWith(timestamp_delta_max - timestamp_delta_min));
        b.add(getBitWith(value_delta_max - value_delta_min));
        return b;
    }

    private static ArrayList<Integer> adjustTon(ArrayList<ArrayList<Integer>> ts_block, int alpha) {
        int block_size = ts_block.size();
        assert alpha != block_size - 1;
        assert alpha != 0;
        ArrayList<Integer> b = new ArrayList<>();
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;

        for (int i = 1; i < block_size; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            if (i == (alpha + 1)) {
                timestamp_delta_i = ts_block.get(alpha + 1).get(0) - ts_block.get(alpha - 1).get(0);
                value_delta_i = ts_block.get(alpha + 1).get(1) - ts_block.get(alpha - 1).get(1);
            } else if (i == alpha) {
                timestamp_delta_i = ts_block.get(alpha).get(0) - ts_block.get(block_size - 1).get(0);
                value_delta_i = ts_block.get(alpha).get(1) - ts_block.get(block_size - 1).get(1);
            } else {
                timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
                value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
            }
            if (timestamp_delta_i > timestamp_delta_max) {
                timestamp_delta_max = timestamp_delta_i;
            }
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i > value_delta_max) {
                value_delta_max = value_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }
        }

        b.add(getBitWith(timestamp_delta_max - timestamp_delta_min));
        b.add(getBitWith(value_delta_max - value_delta_min));
        return b;
    }

    private static ArrayList<Integer> adjustAlphaToJ(
            ArrayList<ArrayList<Integer>> ts_block, int alpha, int j) {

        int block_size = ts_block.size();
        assert alpha != block_size - 1;
        assert alpha != 0;
        assert j != 0;
        assert j != block_size;
        ArrayList<Integer> b = new ArrayList<>();
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;

        for (int i = 1; i < block_size; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            if (i == j) {
                timestamp_delta_i = ts_block.get(j).get(0) - ts_block.get(alpha).get(0);
                value_delta_i = ts_block.get(j).get(1) - ts_block.get(alpha).get(1);
            } else if (i == alpha) {
                timestamp_delta_i = ts_block.get(alpha).get(0) - ts_block.get(j - 1).get(0);
                value_delta_i = ts_block.get(alpha).get(1) - ts_block.get(j - 1).get(1);
            } else if (i == alpha + 1) {
                timestamp_delta_i = ts_block.get(alpha + 1).get(0) - ts_block.get(alpha - 1).get(0);
                value_delta_i = ts_block.get(alpha + 1).get(1) - ts_block.get(alpha - 1).get(1);
            } else {
                timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
                value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
            }
            if (timestamp_delta_i > timestamp_delta_max) {
                timestamp_delta_max = timestamp_delta_i;
            }
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i > value_delta_max) {
                value_delta_max = value_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }
        }
        b.add(getBitWith(timestamp_delta_max - timestamp_delta_min));
        b.add(getBitWith(value_delta_max - value_delta_min));
        return b;
    }

    // adjust n to 0
    private static ArrayList<Integer> adjustn0(ArrayList<ArrayList<Integer>> ts_block) {
        int block_size = ts_block.size();
        ArrayList<Integer> b = new ArrayList<>();
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;

        for (int i = 1; i < block_size - 1; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
            value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
            if (timestamp_delta_i > timestamp_delta_max) {
                timestamp_delta_max = timestamp_delta_i;
            }
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i > value_delta_max) {
                value_delta_max = value_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }
        }
        int timestamp_delta_i;
        int value_delta_i;
        timestamp_delta_i = ts_block.get(0).get(0) - ts_block.get(block_size - 1).get(0);
        value_delta_i = ts_block.get(0).get(1) - ts_block.get(block_size - 1).get(1);
        if (timestamp_delta_i > timestamp_delta_max) {
            timestamp_delta_max = timestamp_delta_i;
        }
        if (timestamp_delta_i < timestamp_delta_min) {
            timestamp_delta_min = timestamp_delta_i;
        }
        if (value_delta_i > value_delta_max) {
            value_delta_max = value_delta_i;
        }
        if (value_delta_i < value_delta_min) {
            value_delta_min = value_delta_i;
        }
        b.add(getBitWith(timestamp_delta_max - timestamp_delta_min));
        b.add(getBitWith(value_delta_max - value_delta_min));
        return b;
    }

    // adjust n to no 0
    private static ArrayList<Integer> adjustn(
            ArrayList<ArrayList<Integer>> ts_block, int alpha, int j) {
        int block_size = ts_block.size();
        assert alpha == block_size - 1;
        assert j != 0;
        ArrayList<Integer> b = new ArrayList<>();
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;

        for (int i = 1; i < block_size - 1; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            if (i != j) {
                timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
                value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
            } else {
                timestamp_delta_i = ts_block.get(j).get(0) - ts_block.get(alpha).get(0);
                value_delta_i = ts_block.get(j).get(1) - ts_block.get(alpha).get(1);
                if (timestamp_delta_i > timestamp_delta_max) {
                    timestamp_delta_max = timestamp_delta_i;
                }
                if (timestamp_delta_i < timestamp_delta_min) {
                    timestamp_delta_min = timestamp_delta_i;
                }
                if (value_delta_i > value_delta_max) {
                    value_delta_max = value_delta_i;
                }
                if (value_delta_i < value_delta_min) {
                    value_delta_min = value_delta_i;
                }
                timestamp_delta_i = ts_block.get(alpha).get(0) - ts_block.get(j - 1).get(0);
                value_delta_i = ts_block.get(alpha).get(1) - ts_block.get(j - 1).get(1);
            }
            if (timestamp_delta_i > timestamp_delta_max) {
                timestamp_delta_max = timestamp_delta_i;
            }
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i > value_delta_max) {
                value_delta_max = value_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }
        }
        b.add(getBitWith(timestamp_delta_max - timestamp_delta_min));
        b.add(getBitWith(value_delta_max - value_delta_min));
        return b;
    }

    private static int getIstarClose(int alpha, ArrayList<Integer> j_star_list) {
        int min_i = 0;
        int min_dis = Integer.MAX_VALUE;
        for (int i : j_star_list) {
            if (abs(alpha - i) < min_dis) {
                min_i = i;
                min_dis = abs(alpha - i);
            }
        }
        if (min_dis == 0) {
            System.out.println("get IstarClose error");
            return 0;
        }
        return min_i;
    }

    // adjust 0 to n
    private static ArrayList<Integer> adjust0n1(ArrayList<ArrayList<Integer>> ts_block) {
        int block_size = ts_block.size();
        ArrayList<Integer> b = new ArrayList<>();
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;

        for (int i = 2; i < block_size; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
            value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
            if (timestamp_delta_i > timestamp_delta_max) {
                timestamp_delta_max = timestamp_delta_i;
            }
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i > value_delta_max) {
                value_delta_max = value_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }
        }
        int timestamp_delta_i;
        int value_delta_i;
        timestamp_delta_i = ts_block.get(0).get(0) - ts_block.get(block_size - 1).get(0);
        value_delta_i = ts_block.get(0).get(1) - ts_block.get(block_size - 1).get(1);
        if (timestamp_delta_i > timestamp_delta_max) {
            timestamp_delta_max = timestamp_delta_i;
        }
        if (timestamp_delta_i < timestamp_delta_min) {
            timestamp_delta_min = timestamp_delta_i;
        }
        if (value_delta_i > value_delta_max) {
            value_delta_max = value_delta_i;
        }
        if (value_delta_i < value_delta_min) {
            value_delta_min = value_delta_i;
        }
        b.add(getBitWith(timestamp_delta_max - timestamp_delta_min));
        b.add(getBitWith(value_delta_max - value_delta_min));
        return b;
    }

    // adjust 0 to no n
    private static ArrayList<Integer> adjust0(
            ArrayList<ArrayList<Integer>> ts_block, int alpha, int j) {
        int block_size = ts_block.size();
        assert alpha == 0;
        assert j != block_size;

        ArrayList<Integer> b = new ArrayList<>();
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;

        for (int i = 2; i < block_size; i++) {
            int timestamp_delta_i;
            int value_delta_i;
            if (i != j) {
                timestamp_delta_i = ts_block.get(i).get(0) - ts_block.get(i - 1).get(0);
                value_delta_i = ts_block.get(i).get(1) - ts_block.get(i - 1).get(1);
            } else {
                timestamp_delta_i = ts_block.get(j).get(0) - ts_block.get(alpha).get(0);
                value_delta_i = ts_block.get(j).get(1) - ts_block.get(alpha).get(1);
                if (timestamp_delta_i > timestamp_delta_max) {
                    timestamp_delta_max = timestamp_delta_i;
                }
                if (timestamp_delta_i < timestamp_delta_min) {
                    timestamp_delta_min = timestamp_delta_i;
                }
                if (value_delta_i > value_delta_max) {
                    value_delta_max = value_delta_i;
                }
                if (value_delta_i < value_delta_min) {
                    value_delta_min = value_delta_i;
                }
                timestamp_delta_i = ts_block.get(alpha).get(0) - ts_block.get(j - 1).get(0);
                value_delta_i = ts_block.get(alpha).get(1) - ts_block.get(j - 1).get(1);
            }
            if (timestamp_delta_i > timestamp_delta_max) {
                timestamp_delta_max = timestamp_delta_i;
            }
            if (timestamp_delta_i < timestamp_delta_min) {
                timestamp_delta_min = timestamp_delta_i;
            }
            if (value_delta_i > value_delta_max) {
                value_delta_max = value_delta_i;
            }
            if (value_delta_i < value_delta_min) {
                value_delta_min = value_delta_i;
            }
        }
        b.add(getBitWith(timestamp_delta_max - timestamp_delta_min));
        b.add(getBitWith(value_delta_max - value_delta_min));
        return b;
    }

    public static int getIStar(ArrayList<ArrayList<Integer>> ts_block, int block_size, int index) {
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int timestamp_delta_max_index = -1;
        int value_delta_max_index = -1;

        int i_star = 0;

        if (index == 0) {
            for (int j = 1; j < block_size; j++) {
                int epsilon_v_j = ts_block.get(j).get(1) - ts_block.get(j - 1).get(1);
                if (epsilon_v_j > value_delta_max) {
                    value_delta_max = epsilon_v_j;
                    value_delta_max_index = j;
                }
            }
            i_star = value_delta_max_index;
        } else if (index == 1) {
            for (int j = 1; j < block_size; j++) {
                int epsilon_r_j = ts_block.get(j).get(0) - ts_block.get(j - 1).get(0);
                if (epsilon_r_j > timestamp_delta_max) {
                    timestamp_delta_max = epsilon_r_j;
                    timestamp_delta_max_index = j;
                }
            }
            i_star = timestamp_delta_max_index;
        }

        return i_star;
    }

    public static int getIStar(
            ArrayList<ArrayList<Integer>> ts_block, int block_size, ArrayList<Integer> raw_length) {
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int timestamp_delta_max_index = -1;
        int value_delta_max_index = -1;

        int i_star_bit_width = 33;
        int i_star = 0;

        for (int j = 1; j < block_size; j++) {
            int epsilon_r_j = ts_block.get(j).get(0) - ts_block.get(j - 1).get(0);
            int epsilon_v_j = ts_block.get(j).get(1) - ts_block.get(j - 1).get(1);

            if (epsilon_r_j > timestamp_delta_max) {
                timestamp_delta_max = epsilon_r_j;
                timestamp_delta_max_index = j;
            }
            if (epsilon_r_j < timestamp_delta_min) {
                timestamp_delta_min = epsilon_r_j;
            }
            if (epsilon_v_j > value_delta_max) {
                value_delta_max = epsilon_v_j;
                value_delta_max_index = j;
            }
            if (epsilon_v_j < value_delta_min) {
                value_delta_min = epsilon_v_j;
            }
        }
        timestamp_delta_max -= timestamp_delta_min;
        value_delta_max -= value_delta_min;

        if (value_delta_max <= timestamp_delta_max) i_star = timestamp_delta_max_index;
        else i_star = value_delta_max_index;
        return i_star;
    }

    public static ArrayList<Byte> encode2Bytes(
            ArrayList<ArrayList<Integer>> ts_block,
            ArrayList<Integer> raw_length,
            ArrayList<Integer> result2) {
        ArrayList<Byte> encoded_result = new ArrayList<>();

        // encode interval0 and value0
        byte[] interval0_byte = int2Bytes(ts_block.get(0).get(0));
        for (byte b : interval0_byte) encoded_result.add(b);
        byte[] value0_byte = int2Bytes(ts_block.get(0).get(1));
        for (byte b : value0_byte) encoded_result.add(b);

        // encode theta
        byte[] timestamp_min_byte = int2Bytes(raw_length.get(3));
        for (byte b : timestamp_min_byte) encoded_result.add(b);
        byte[] value_min_byte = int2Bytes(raw_length.get(4));
        for (byte b : value_min_byte) encoded_result.add(b);

        // encode interval
        byte[] max_bit_width_interval_byte = bitWidth2Bytes(raw_length.get(1));
        for (byte b : max_bit_width_interval_byte) encoded_result.add(b);
        byte[] timestamp_bytes = bitPacking(ts_block, 0, raw_length.get(1));
        for (byte b : timestamp_bytes) encoded_result.add(b);

        // encode value
        byte[] max_bit_width_value_byte = bitWidth2Bytes(raw_length.get(2));
        for (byte b : max_bit_width_value_byte) encoded_result.add(b);
        byte[] value_bytes = bitPacking(ts_block, 1, raw_length.get(2));
        for (byte b : value_bytes) encoded_result.add(b);

//        byte[] td_common_byte = int2Bytes(result2.get(0));
//        for (byte b : td_common_byte) encoded_result.add(b);

        return encoded_result;
    }

    public static ArrayList<Byte> encodeSegmentMeta2Bytes(
            ArrayList<ArrayList<Integer>> ts_block,
            ArrayList<Integer> raw_length,
            ArrayList<Integer> result2) {
        ArrayList<Byte> encoded_result = new ArrayList<>();

        // encode interval0 and value0
        byte[] interval0_byte = int2Bytes(ts_block.get(0).get(0));
        for (byte b : interval0_byte) encoded_result.add(b);
        byte[] value0_byte = int2Bytes(ts_block.get(0).get(1));
        for (byte b : value0_byte) encoded_result.add(b);

        // encode theta
        byte[] timestamp_min_byte = int2Bytes(raw_length.get(3));
        for (byte b : timestamp_min_byte) encoded_result.add(b);
        byte[] value_min_byte = int2Bytes(raw_length.get(4));
        for (byte b : value_min_byte) encoded_result.add(b);

        // encode interval
        byte[] max_bit_width_interval_byte = bitWidth2Bytes(raw_length.get(1));
        for (byte b : max_bit_width_interval_byte) encoded_result.add(b);
        byte[] timestamp_bytes = bitPacking(ts_block, 0, raw_length.get(1));
        for (byte b : timestamp_bytes) encoded_result.add(b);

        // encode value
        byte[] max_bit_width_value_byte = bitWidth2Bytes(raw_length.get(2));
        for (byte b : max_bit_width_value_byte) encoded_result.add(b);
        byte[] value_bytes = bitPacking(ts_block, 1, raw_length.get(2));
        for (byte b : value_bytes) encoded_result.add(b);

//        byte[] td_common_byte = int2Bytes(result2.get(0));
//        for (byte b : td_common_byte) encoded_result.add(b);

        return encoded_result;
    }

    public static ArrayList<Byte> encodeSegment2Bytes(
            ArrayList<ArrayList<Integer>> delta_segments,
            ArrayList<ArrayList<Integer>> bit_width_segments,
            int segment_size) {
        ArrayList<Byte> encoded_result = new ArrayList<>();
        int block_size = delta_segments.size();
        int segment_n = block_size/segment_size;
        System.out.println("segment_n"+segment_n);
        System.out.println("block_size"+block_size);

        for(int segment_i = 0;segment_i<segment_n;segment_i++){
            int bit_width_time = bit_width_segments.get(segment_i).get(0);
            int bit_width_value = bit_width_segments.get(segment_i).get(1);
            byte[] timestamp_bytes = bitPacking(delta_segments, 0,segment_i*segment_size,segment_size, bit_width_segments.get(segment_i).get(0));
            for (byte b : timestamp_bytes) encoded_result.add(b);
            byte[] value_bytes = bitPacking(delta_segments, 1, segment_i*segment_size,segment_size,bit_width_segments.get(segment_i).get(1));
            for (byte b : value_bytes) encoded_result.add(b);
        }


//        byte[] td_common_byte = int2Bytes(result2.get(0));
//        for (byte b : td_common_byte) encoded_result.add(b);

        return encoded_result;
    }
    public static ArrayList<Byte> encodeBitWidth2Bytes(
            ArrayList<ArrayList<Integer>> bit_width_segments) {
        ArrayList<Byte> encoded_result = new ArrayList<>();
        for (ArrayList<Integer> bit_width_segment : bit_width_segments) {
            byte[] timestamp_bytes = bitWidth2Bytes(bit_width_segment.get(0));
            for (byte b : timestamp_bytes) encoded_result.add(b);
            byte[] value_bytes = bitWidth2Bytes(bit_width_segment.get(1));
            for (byte b : value_bytes) encoded_result.add(b);
        }

        return encoded_result;
    }
    public static void getEncodeBitsRegressionSegment(
            ArrayList<ArrayList<Integer>> ts_block,
            ArrayList<ArrayList<Integer>> bit_width_segments,
            ArrayList<ArrayList<Integer>> first_data_segments,
            ArrayList<ArrayList<Integer>> delta_min_segments,
            ArrayList<ArrayList<Integer>> delta_segments
    ) {
        int block_size = ts_block.size();
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();


        // delta to Regression
        for (int j = 1; j < block_size; j++) {
            int epsilon_r = ts_block.get(j).get(0) - ts_block.get(j - 1).get(0);
            int epsilon_v = ts_block.get(j).get(1) - ts_block.get(j - 1).get(1);

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
        for (int j = 0; j < block_size-1; j++) {
            int epsilon_r = ts_block_delta.get(j).get(0) - timestamp_delta_min;
            int epsilon_v = ts_block_delta.get(j).get(1) - value_delta_min;
            if (epsilon_r > max_interval) {
                max_interval = epsilon_r;
            }
            if (epsilon_v > max_value) {
                max_value = epsilon_v;
            }
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add(epsilon_r);
            tmp.add(epsilon_v);
            delta_segments.add(tmp);
        }
        int max_bit_width_interval = getBitWith(max_interval);
        int max_bit_width_value = getBitWith(max_value);

        ArrayList<Integer> tmp = new ArrayList<>();
        tmp.add(max_bit_width_interval);
        tmp.add(max_bit_width_value);
        bit_width_segments.add(tmp);

        tmp = new ArrayList<>();
        tmp.add(timestamp_delta_min);
        tmp.add(value_delta_min);
        delta_min_segments.add(tmp);

        ArrayList<Integer> tmp0 = new ArrayList<>();
        tmp0.add(ts_block.get(0).get(0));
        tmp0.add(ts_block.get(0).get(1));
        first_data_segments.add(tmp0);


    }

    public static ArrayList<Byte> ReorderingRegressionEncoder(
            ArrayList<ArrayList<Integer>> data, int block_size) throws IOException {
//    block_size++;
        ArrayList<Byte> encoded_result = new ArrayList<Byte>();
        int length_all = data.size();
        byte[] length_all_bytes = int2Bytes(length_all);
        for (byte b : length_all_bytes) encoded_result.add(b);
        int block_num = length_all / block_size;

        // encode block size (Integer)
        byte[] block_size_byte = int2Bytes(block_size);
        for (byte b : block_size_byte) encoded_result.add(b);
        //    String Output_before
        // ="C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\vldb\\bit-width\\bit-width-before-test.csv";
        //    String Output_after
        // ="C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\vldb\\bit-width\\bit-width-after-test.csv";
        //    CsvWriter writer_before = new CsvWriter(Output_before, ',', StandardCharsets.UTF_8);
        //    CsvWriter writer_after = new CsvWriter(Output_after, ',', StandardCharsets.UTF_8);
        //    String[] head = {
        //            "Timestamp bit width",
        //            "Value bit width"
        //    };
        //    writer_before.writeRecord(head); // write header to output file
        //    writer_after.writeRecord(head);

        int count_raw = 0;
        int count_reorder = 0;
        //    System.out.println(block_num);
//        for (int i = 0; i < 1; i++) {
    for (int i = 0; i < block_num; i++) {
            ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();
            for (int j = 0; j < block_size; j++) {
                ts_block.add(data.get(j + i * block_size));
                ts_block_reorder.add(data.get(j + i * block_size));
            }
            //      System.out.println("ts_block:"+ts_block);
            ArrayList<Integer> result2 = new ArrayList<>();
            splitTimeStamp3(ts_block, result2);

            //      System.out.println("result2:"+result2);

            quickSort(ts_block, 0, 0, block_size - 1);
            // System.out.println(i);

            // time-order
            ArrayList<Integer> raw_length = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegression(ts_block, block_size, raw_length);
            //      for (int j=1;j< ts_block_delta.size();j++) {
            //        ArrayList<Integer> integers = ts_block_delta.get(j);
            //        head = new String[]{
            //                String.valueOf(getBitWith(integers.get(0))),
            //                String.valueOf(getBitWith(integers.get(1)))
            //        };
            //        writer_before.writeRecord(head);
            //      }
            // value-order
            quickSort(ts_block, 1, 0, block_size - 1);
            ArrayList<Integer> reorder_length = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta_reorder =
                    getEncodeBitsRegression(ts_block, block_size, reorder_length);
            //      for (int j=1;j< ts_block_delta_reorder.size();j++) {
            //        ArrayList<Integer> integers = ts_block_delta_reorder.get(j);
            //        head = new String[]{
            //                String.valueOf(integers.get(0)),
            //                String.valueOf(getBitWith(integers.get(1)))
            //        };
            //        writer_after.writeRecord(head);
            //      }

            int i_star;
            int j_star;
            if (raw_length.get(0) <= reorder_length.get(0)) {
                quickSort(ts_block, 0, 0, block_size - 1);
                count_raw++;
                i_star = getIStar(ts_block, block_size, 0);
            } else {
                raw_length = reorder_length;
                quickSort(ts_block, 1, 0, block_size - 1);
                count_reorder++;
                i_star = getIStar(ts_block, block_size, 1);
            }

            j_star = getJStar(ts_block, i_star, block_size, raw_length, 0);

//      System.out.println("i_star"+i_star);
//          System.out.println("j_star"+j_star);

            int adjust_count = 0;
            while (j_star != -1 && i_star != -1) {
                if (adjust_count < block_size / 2 || adjust_count <= 33) {
                    adjust_count++;
                } else {
                    break;
                }
                ArrayList<ArrayList<Integer>> old_ts_block =
                        (ArrayList<ArrayList<Integer>>) ts_block.clone();
                ArrayList<Integer> old_length = (ArrayList<Integer>) raw_length.clone();

                ArrayList<Integer> tmp_tv = ts_block.get(i_star);
                if (j_star < i_star) {
                    for (int u = i_star - 1; u >= j_star; u--) {
                        ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
                        tmp_tv_cur.add(ts_block.get(u).get(0));
                        tmp_tv_cur.add(ts_block.get(u).get(1));
                        ts_block.set(u + 1, tmp_tv_cur);
                    }
                } else {
                    for (int u = i_star + 1; u < j_star; u++) {
                        ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
                        tmp_tv_cur.add(ts_block.get(u).get(0));
                        tmp_tv_cur.add(ts_block.get(u).get(1));
                        ts_block.set(u - 1, tmp_tv_cur);
                    }
                    j_star--;
                }
                ts_block.set(j_star, tmp_tv);

                getEncodeBitsRegression(ts_block, block_size, raw_length);
                if (old_length.get(1) + old_length.get(2) < raw_length.get(1) + raw_length.get(2)) {
                    ts_block = old_ts_block;
                    break;
                }

                i_star = getIStar(ts_block, block_size, raw_length);
                if (i_star == j_star) break;
                j_star = getJStar(ts_block, i_star, block_size, raw_length, 0);
//        System.out.println("i_star"+i_star);
//        System.out.println("j_star"+j_star);

            }


            ArrayList<ArrayList<Integer>> bit_width_segments = new ArrayList<>();
            ArrayList<ArrayList<Integer>> first_data_segments = new ArrayList<>();
            ArrayList<ArrayList<Integer>> delta_min_segments = new ArrayList<>();
            ArrayList<ArrayList<Integer>> delta_segments = new ArrayList<>();
            int segment_n = block_size / 17;
            for (int segment_i = 0; segment_i < segment_n; segment_i++) {
                ArrayList<ArrayList<Integer>> segment_data = new ArrayList<>();
                ArrayList<ArrayList<Integer>> segment_data_reorder = new ArrayList<>();
                for (int data_i = segment_i * 17; data_i < (segment_i + 1) * 17; data_i++) {
                    segment_data.add(ts_block.get(data_i));
                }
                quickSort(segment_data, 0, 0, 16);
                ArrayList<Integer> segment_data_length = new ArrayList<>();
                ArrayList<ArrayList<Integer>> segment_data_delta = getEncodeBitsRegression(segment_data, 17, segment_data_length);

                quickSort(segment_data, 1, 0, 16);
                ArrayList<Integer> segment_data_length_reorder = new ArrayList<>();
                ArrayList<ArrayList<Integer>> segment_data_delta_reorder = getEncodeBitsRegression(segment_data, 17, segment_data_length_reorder);

                int alpha;
                int beta;
                if (segment_data_length.get(0) <= segment_data_length_reorder.get(0)) {
                    quickSort(segment_data, 0, 0, 16);
//          count_raw++;
                    alpha = getIStar(segment_data, 16, 0);
                } else {
                    segment_data_length = segment_data_length_reorder;
//          count_reorder++;
                    alpha = getIStar(segment_data, 16, 1);
                }
                beta = getJStar(segment_data, alpha, 17, segment_data_length, 0);

                int adjust_count_segment = 0;
                while (beta != -1 && alpha != -1) {
                    if (adjust_count_segment < 16) {
                        adjust_count_segment++;
                    } else {
                        break;
                    }
                    ArrayList<ArrayList<Integer>> old_segment = (ArrayList<ArrayList<Integer>>) segment_data.clone();
                    ArrayList<Integer> old_length = (ArrayList<Integer>) segment_data_length.clone();

                    ArrayList<Integer> tmp_tv = segment_data.get(alpha);
                    if (beta < alpha) {
                        for (int u = alpha - 1; u >= beta; u--) {
                            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
                            tmp_tv_cur.add(segment_data.get(u).get(0));
                            tmp_tv_cur.add(segment_data.get(u).get(1));
                            segment_data.set(u + 1, tmp_tv_cur);
                        }
                    } else {
                        for (int u = alpha + 1; u < beta; u++) {
                            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
                            tmp_tv_cur.add(segment_data.get(u).get(0));
                            tmp_tv_cur.add(segment_data.get(u).get(1));
                            segment_data.set(u - 1, tmp_tv_cur);
                        }
                        beta--;
                    }
                    segment_data.set(beta, tmp_tv);

                    getEncodeBitsRegression(segment_data, 17, segment_data_length);
                    if (old_length.get(1) + old_length.get(2) < segment_data_length.get(1) + segment_data_length.get(2)) {
                        segment_data = old_segment;
                        break;
                    }

                    alpha = getIStar(segment_data, 17, segment_data_length);
                    if (alpha == beta) break;
                    beta = getJStar(segment_data, alpha, 17, segment_data_length, 0);
//        System.out.println("i_star"+i_star);
//        System.out.println("j_star"+j_star);
                }
                getEncodeBitsRegressionSegment(segment_data, bit_width_segments, first_data_segments, delta_min_segments, delta_segments);
            }

            ArrayList<Integer> meta_length = new ArrayList<>();
            ArrayList<ArrayList<Integer>> first_data_segments_delta = getEncodeBitsRegression(first_data_segments, first_data_segments.size(), meta_length);
            ArrayList<Byte> cur_encoded_result = encode2Bytes(first_data_segments_delta, meta_length, result2);
            encoded_result.addAll(cur_encoded_result);

            ArrayList<ArrayList<Integer>> delta_min_segments_delta = getEncodeBitsRegression(delta_min_segments, delta_min_segments.size(), meta_length);
            cur_encoded_result = encode2Bytes(first_data_segments_delta, meta_length, result2);
            encoded_result.addAll(cur_encoded_result);


            cur_encoded_result = encodeSegment2Bytes(delta_segments,bit_width_segments,16);
            encoded_result.addAll(cur_encoded_result);

            cur_encoded_result = encodeBitWidth2Bytes(bit_width_segments);
            encoded_result.addAll(cur_encoded_result);



//            ts_block_delta = getEncodeBitsRegression(ts_block, block_size, raw_length);
////      System.out.println(ts_block);
//            ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta, raw_length, result2);
//            encoded_result.addAll(cur_encoded_result);

//      System.out.println("encoded_result: "+(encoded_result.size()*8));
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

            for (int j = block_num * block_size; j < length_all; j++) {
                ts_block.add(data.get(j));
                ts_block_reorder.add(data.get(j));
            }
            ArrayList<Integer> result2 = new ArrayList<>();
            splitTimeStamp3(ts_block, result2);

            quickSort(ts_block, 0, 0, remaining_length - 1);

            // time-order
            ArrayList<Integer> raw_length =
                    new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
            ArrayList<ArrayList<Integer>> ts_block_delta =
                    getEncodeBitsRegression(ts_block, remaining_length, raw_length);

            // value-order
            quickSort(ts_block, 1, 0, remaining_length - 1);
            ArrayList<Integer> reorder_length = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta_reorder =
                    getEncodeBitsRegression(ts_block, remaining_length, reorder_length);

            if (raw_length.get(0) <= reorder_length.get(0)) {
                quickSort(ts_block, 0, 0, remaining_length - 1);
                count_raw++;
            } else {
                raw_length = reorder_length;
                quickSort(ts_block, 1, 0, remaining_length - 1);
                count_reorder++;
            }
            ts_block_delta =
                    getEncodeBitsRegression(ts_block, remaining_length, raw_length);
            int supple_length;
            if (remaining_length % 8 == 0) {
                supple_length = 1;
            } else if (remaining_length % 8 == 1) {
                supple_length = 0;
            } else {
                supple_length = 9 - remaining_length % 8;
            }
            for (int s = 0; s < supple_length; s++) {
                ArrayList<Integer> tmp = new ArrayList<>();
                tmp.add(0);
                tmp.add(0);
                ts_block_delta.add(tmp);
            }
            ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta, raw_length, result2);
            encoded_result.addAll(cur_encoded_result);
        }
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

            quickSort(ts_block, 0, 0, remain_length + zero_number - 1);

            for (int i = zero_number; i < remain_length + zero_number; i++) {
                data.add(ts_block.get(i));
            }
        }
        return data;
    }

    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
//        String parent_dir =
//                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\vldb\\compression_ratio\\delta_reordering_ratio";
        String parent_dir = "C:\\Users\\xiaoj\\Desktop\\test";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        //    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\vldb\\test");
        //    output_path_list.add("C:\\Users\\xiaoj\\Desktop\\test.csv");
        //    dataset_block_size.add(1024);

        input_path_list.add(
                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\EPM-Education");
        output_path_list.add(parent_dir + "\\EPM-Education_ratio.csv");
        dataset_block_size.add(512);
        input_path_list.add(
                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\Vehicle-Charge");
        output_path_list.add(parent_dir + "\\Vehicle-Charge_ratio.csv");
        dataset_block_size.add(512);

//    input_path_list.add(
//        "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\CS-Sensors");
//    output_path_list.add(parent_dir + "\\CS-Sensors_ratio.csv");
//    dataset_block_size.add(1024);
        input_path_list.add(
                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\Metro-Traffic");
        output_path_list.add(parent_dir + "\\Metro-Traffic_ratio.csv");
//    dataset_block_size.add(512);
        dataset_block_size.add(561);
        input_path_list.add(
                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\Nifty-Stocks");
        output_path_list.add(parent_dir + "\\Nifty-Stocks_ratio.csv");
        dataset_block_size.add(256);
        input_path_list.add(
                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\USGS-Earthquakes");
        output_path_list.add(parent_dir + "\\USGS-Earthquakes_ratio.csv");
        dataset_block_size.add(512);
        input_path_list.add(
                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\Cyber-Vehicle");
        output_path_list.add(parent_dir + "\\Cyber-Vehicle_ratio.csv");
        dataset_block_size.add(128);
        input_path_list.add(
                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\TH-Climate");
        output_path_list.add(parent_dir + "\\TH-Climate_ratio.csv");
        dataset_block_size.add(512);
        input_path_list.add(
                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\TY-Transport");
        output_path_list.add(parent_dir + "\\TY-Transport_ratio.csv");
        dataset_block_size.add(512);
        input_path_list.add(
                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\TY-Fuel");
        output_path_list.add(parent_dir + "\\TY-Fuel_ratio.csv");
        dataset_block_size.add(64);
        input_path_list.add(
                "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\GW-Magnetic");
        output_path_list.add(parent_dir + "\\GW-Magnetic_ratio.csv");
        dataset_block_size.add(128);

        for(int file_i=2;file_i<3;file_i++){
//        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

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
                    //          System.out.println(tmp);
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
                        buffer = ReorderingRegressionEncoder(data, dataset_block_size.get(file_i));
                    long e = System.nanoTime();
                    encodeTime += ((e - s) / repeatTime2);
                    compressed_size += buffer.size();
                    double ratioTmp = (double) buffer.size() / (double) (data.size() * Integer.BYTES * 2);
                    ratio += ratioTmp;
                    s = System.nanoTime();
                    //          for(int repeat=0;repeat<repeatTime2;repeat++)
                    //            data_decoded = ReorderingRegressionDecoder(buffer);
                    e = System.nanoTime();
                    decodeTime += ((e - s) / repeatTime2);
                }

                ratio /= repeatTime;
                compressed_size /= repeatTime;
                encodeTime /= repeatTime;
                decodeTime /= repeatTime;

                String[] record = {
                        f.toString(),
                        "Delta-Reordering",
                        String.valueOf(encodeTime),
                        String.valueOf(decodeTime),
                        String.valueOf(data.size()),
                        String.valueOf(compressed_size),
                        String.valueOf(ratio)
                };
                System.out.println(ratio);
                writer.writeRecord(record);
//        break;
            }
            writer.close();
        }
    }
}
