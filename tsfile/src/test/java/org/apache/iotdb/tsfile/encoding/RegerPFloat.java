package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Stack;

import static java.lang.Math.abs;

public class RegerPFloat {



    public static int min3(int a, int b, int c) {
        if (a < b && a < c) {
            return 0;
        } else if (b < c) {
            return 1;
        } else {
            return 2;
        }
    }

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
        for (int i = 0; i < (block_size - 1) / 8; i++) {
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

            tmp.add(interval_i);
            tmp.add(ts_block.get(i).get(1));
            ts_block.set(i, tmp);


        }
        ArrayList<Integer> tmp = new ArrayList<>();

        tmp.add(0);
        tmp.add(ts_block.get(0).get(1));
        ts_block.set(0, tmp);

        result.add(td_common);
        result.add(t0);
    }

    public static void terminate(
            ArrayList<ArrayList<Integer>> ts_block, ArrayList<Float> coefficient, int p) {
        int length = ts_block.size();
        assert length > p;
        int size = length - p;



        double[] param;
        try {
            OLSMultipleLinearRegression ols1 = new OLSMultipleLinearRegression();
            double[][] X1 = new double[size][p];
            double[] Y1 = new double[size];
            for (int i = 0; i < size; i++) {
                X1[i] = new double[p];
                for (int j = 0; j < p; j++) {
                    X1[i][j] = ts_block.get(i + j).get(0);
                }
                Y1[i] = ts_block.get(i + p).get(0);
            }
            ols1.newSampleData(Y1, X1);
            param = ols1.estimateRegressionParameters(); //结果的第1项是常数项， 之后依次序为各个特征的系数
            //System.out.println(Arrays.toString(param));
        } catch (Exception e) {
            param = new double[p + 1];
            for (int i = 0; i <= p; i++) {
                param[i] = 0;
            }
        }



        double[] param2;
        try {
            OLSMultipleLinearRegression ols2 = new OLSMultipleLinearRegression();
            double[][] X2 = new double[size][p];
            double[] Y2 = new double[size];
            for (int i = 0; i < size; i++) {
                X2[i] = new double[p];
                for (int j = 0; j < p; j++) {
                    X2[i][j] = ts_block.get(i + j).get(1);
                }
                Y2[i] = ts_block.get(i + p).get(1);
            }
            ols2.newSampleData(Y2, X2);
            param2 = ols2.estimateRegressionParameters(); //结果的第1项是常数项， 之后依次序为各个特征的系数
            //System.out.println(Arrays.toString(param2));
        } catch (Exception exception) {
            param2 = new double[p + 1];
            for (int i = 0; i <= p; i++) {
                param2[i] = 0;
            }
        }

        for (int i = 0; i <= p; i++) {
            coefficient.add((float) param[i]);
            coefficient.add((float) param2[i]);
        }
    }

    // --------------------------------------  base function -----------------------------------------------------
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

    public static void moveAlphaToBeta(ArrayList<ArrayList<Integer>> ts_block, int alpha, int beta) {
        ArrayList<Integer> tmp_tv = ts_block.get(alpha);
        if (beta < alpha) {
            for (int u = alpha - 1; u >= beta; u--) {
                ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
                tmp_tv_cur.add(ts_block.get(u).get(0));
                tmp_tv_cur.add(ts_block.get(u).get(1));
                ts_block.set(u + 1, tmp_tv_cur);
            }
        } else {
            for (int u = alpha + 1; u < beta; u++) {
                ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
                tmp_tv_cur.add(ts_block.get(u).get(0));
                tmp_tv_cur.add(ts_block.get(u).get(1));
                ts_block.set(u - 1, tmp_tv_cur);
            }
            beta--;
        }
        ts_block.set(beta, tmp_tv);
    }

    private static ArrayList<Integer> isMovable(ArrayList<Integer> alpha_list, ArrayList<Integer> beta_list) {
        ArrayList<Integer> isMoveable = new ArrayList<>();
        for (int i = 0; i < alpha_list.size(); i++) {
            if (alpha_list.get(i) != -1 && beta_list.get(i) != -1) {
                isMoveable.add(i);
            }
        }
        return isMoveable;
    }

    private static ArrayList<ArrayList<Integer>> getEncodeBitsRegressionP(
            ArrayList<ArrayList<Integer>> ts_block,
            int block_size,
            ArrayList<Integer> raw_length,
            ArrayList<Float> coefficient,
            int p) {
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int max_timestamp = Integer.MIN_VALUE;
        int max_timestamp_i = -1;
        int max_value = Integer.MIN_VALUE;
        int max_value_i = -1;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
        coefficient.clear();

        terminate(ts_block, coefficient, p);

        ArrayList<Integer> tmp0 = new ArrayList<>();
        tmp0.add(ts_block.get(0).get(0));
        tmp0.add(ts_block.get(0).get(1));
        ts_block_delta.add(tmp0);
        // regression residual
        for (int j = 1; j < p; j++) {
            float epsilon_r = (float) ((float) ts_block.get(j).get(0) - coefficient.get(0));
            float epsilon_v = (float) ((float) ts_block.get(j).get(1) - coefficient.get(1));
            for (int pi = 1; pi <= j; pi++) {
                epsilon_r -= (float) (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));
                epsilon_v -= (float) (coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1));
            }


            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add((int) epsilon_r);
            tmp.add((int) epsilon_v);
            ts_block_delta.add(tmp);
        }

        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ts_block.get(j).get(0) - coefficient.get(0);
            float epsilon_v = (float) ts_block.get(j).get(1) - coefficient.get(1);
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0);
                epsilon_v -= coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1);
            }

            if (epsilon_r < timestamp_delta_min) {
                timestamp_delta_min = (int) epsilon_r;
            }
            if (epsilon_v < value_delta_min) {
                value_delta_min = (int) epsilon_v;
            }
            if (epsilon_r > max_timestamp) {
                max_timestamp = (int) epsilon_r;
            }
            if (epsilon_v > max_value) {
                max_value = (int) epsilon_v;
            }
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add((int) epsilon_r);
            tmp.add((int) epsilon_v);
            ts_block_delta.add(tmp);
        }
        int length = 0;
        for (int j = block_size - 1; j >= p; j--) {
            float epsilon_r = ts_block_delta.get(j).get(0) - timestamp_delta_min;
            float epsilon_v = ts_block_delta.get(j).get(1) - value_delta_min;
            length += getBitWith((int) epsilon_r);
            length += getBitWith((int) epsilon_v);
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add((int) epsilon_r);
            tmp.add((int) epsilon_v);
            ts_block_delta.set(j, tmp);
        }
        int max_bit_width_interval = getBitWith(max_timestamp - timestamp_delta_min);
        int max_bit_width_value = getBitWith(max_value - value_delta_min);
//        int length = (max_bit_width_interval + max_bit_width_value) * (block_size - 1);
        raw_length.clear();

        raw_length.add(length);
        raw_length.add(max_bit_width_interval);
        raw_length.add(max_bit_width_value);

        raw_length.add(timestamp_delta_min);
        raw_length.add(value_delta_min);

        return ts_block_delta;
    }

    private static ArrayList<ArrayList<Integer>> getEncodeBitsRegressionPNoTrain(
            ArrayList<ArrayList<Integer>> ts_block,
            int block_size,
            ArrayList<Integer> raw_length,
            ArrayList<Float> coefficient,
            int p) {
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int max_timestamp = Integer.MIN_VALUE;
        int max_timestamp_i = -1;
        int max_value = Integer.MIN_VALUE;
        int max_value_i = -1;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

        ArrayList<Integer> tmp0 = new ArrayList<>();
        tmp0.add(ts_block.get(0).get(0));
        tmp0.add(ts_block.get(0).get(1));
        ts_block_delta.add(tmp0);
        // regression residual
        for (int j = 1; j < p; j++) {
            float epsilon_r = (float) ((float) ts_block.get(j).get(0) - coefficient.get(0));
            float epsilon_v = (float) ((float) ts_block.get(j).get(1) - coefficient.get(1));
            for (int pi = 1; pi <= j; pi++) {
                epsilon_r -= (float) (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));
                epsilon_v -= (float) (coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1));
            }


            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add((int) epsilon_r);
            tmp.add((int) epsilon_v);
            ts_block_delta.add(tmp);
        }

        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ts_block.get(j).get(0) - coefficient.get(0);
            float epsilon_v = (float) ts_block.get(j).get(1) - coefficient.get(1);
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0);
                epsilon_v -= coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1);
            }

            if (epsilon_r < timestamp_delta_min) {
                timestamp_delta_min = (int) epsilon_r;
            }
            if (epsilon_v < value_delta_min) {
                value_delta_min = (int) epsilon_v;
            }
            if (epsilon_r > max_timestamp) {
                max_timestamp = (int) epsilon_r;
            }
            if (epsilon_v > max_value) {
                max_value = (int) epsilon_v;
            }
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add((int) epsilon_r);
            tmp.add((int) epsilon_v);
            ts_block_delta.add(tmp);
        }
        int length = 0;
        for (int j = block_size - 1; j >= p; j--) {
            float epsilon_r = ts_block_delta.get(j).get(0) - timestamp_delta_min;
            float epsilon_v = ts_block_delta.get(j).get(1) - value_delta_min;
            length += getBitWith((int) epsilon_r);
            length += getBitWith((int) epsilon_v);
            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add((int) epsilon_r);
            tmp.add((int) epsilon_v);
            ts_block_delta.set(j, tmp);
        }
        int max_bit_width_interval = getBitWith(max_timestamp - timestamp_delta_min);
        int max_bit_width_value = getBitWith(max_value - value_delta_min);

        raw_length.clear();

        raw_length.add(length);
        raw_length.add(max_bit_width_interval);
        raw_length.add(max_bit_width_value);

        raw_length.add(timestamp_delta_min);
        raw_length.add(value_delta_min);

        return ts_block_delta;
    }

    public static int getBetaP(
            ArrayList<ArrayList<Integer>> ts_block,
            int alpha,
            int block_size,
            ArrayList<Integer> raw_length,
            ArrayList<Float> coefficient,
            int p) {
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int max_timestamp = Integer.MIN_VALUE;
        int max_value = Integer.MIN_VALUE;
        int raw_timestamp_delta_max_index = -1;
        int raw_value_delta_max_index = -1;

        int raw_abs_sum = raw_length.get(0);



        ArrayList<Integer> j_star_list = new ArrayList<>(); // beta list of min b phi alpha to j
        ArrayList<Integer> max_index = new ArrayList<>();
        int j_star = -1;

        if (alpha == -1) {
            return j_star;
        }

        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ts_block.get(j).get(0) - coefficient.get(0);
            float epsilon_v = (float) ts_block.get(j).get(1) - coefficient.get(1);
//            System.out.println("p="+p);
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));
                epsilon_v -= (coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1));
            }
            if (j != alpha && ((int) epsilon_r == max_timestamp || (int) epsilon_v == max_value)) {
                max_index.add(j);
            }
        }

        // alpha <= p
        if (alpha < p) {

            int j = 0;
            for (; j < alpha; j++) {

                ArrayList<Integer> b = adjustCase2(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }
            for (j = alpha + 2; j < alpha + p; j++) {

                ArrayList<Integer> b = adjustCase3(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }
            for (; j < block_size; j++) {

                ArrayList<Integer> b = adjustCase4(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }
            ArrayList<Integer> b = adjustCase5(ts_block, alpha, coefficient, p);
            if (b.get(0) < raw_abs_sum) {
                raw_abs_sum = b.get(0);
                j_star_list.clear();
                j_star_list.add(block_size);

            } else if (b.get(0) == raw_abs_sum) {
                j_star_list.add(block_size);
            }

        } // alpha == n
        else if (alpha < block_size && alpha >= block_size - p) {

            ArrayList<Integer> b;
            int j = 0;
            for (; j < alpha - p; j++) {

                b = adjustCase1(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }
            for (; j < alpha; j++) {

                b = adjustCase2(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }
            for (j = alpha + 2; j < alpha + p && j < block_size; j++) {

                b = adjustCase3(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }
            for (; j < block_size; j++) {

                b = adjustCase4(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }

            b = adjustCase5(ts_block, alpha, coefficient, p);
            if (b.get(0) < raw_abs_sum) {
                raw_abs_sum = b.get(0);
                j_star_list.clear();
                j_star_list.add(0);
            } else if (b.get(0) == raw_abs_sum) {
                j_star_list.add(0);
            }
        } // p < alpha <= n-p
        else {
            ArrayList<Integer> b;
            int j = 0;
            for (; j < alpha - p; j++) {

                b = adjustCase1(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }
            for (; j < alpha; j++) {

                b = adjustCase2(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }
            for (j = alpha + 2; j < alpha + p && j < block_size; j++) {

                b = adjustCase3(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }
            for (; j < block_size; j++) {

                b = adjustCase4(ts_block, alpha, j, coefficient, p);
                if (b.get(0) < raw_abs_sum) {
                    raw_abs_sum = b.get(0);
                    j_star_list.clear();
                    j_star_list.add(j);
                } else if (b.get(0) == raw_abs_sum) {
                    j_star_list.add(j);
                }
            }
            b = adjustCase5(ts_block, alpha, coefficient, p);
            if (b.get(0) < raw_abs_sum) {
                raw_abs_sum = b.get(0);
                j_star_list.clear();
                j_star_list.add(0);
            } else if (b.get(0) == raw_abs_sum) {
                j_star_list.add(0);
            }
        }

        if (j_star_list.size() != 0) {
            j_star = getIstarClose(alpha, j_star_list);
        }
        return j_star;
    }

    private static ArrayList<Integer> adjustCase1(
            ArrayList<ArrayList<Integer>> ts_block,
            int alpha,
            int j_star,
            ArrayList<Float> coefficient,
            int p) {
        ArrayList<ArrayList<Integer>> tmp_ts_block = (ArrayList<ArrayList<Integer>>) ts_block.clone();
        int block_size = ts_block.size();
        ArrayList<Integer> tmp_tv = tmp_ts_block.get(alpha);
        for (int u = alpha - 1; u >= j_star; u--) {
            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
            tmp_tv_cur.add(tmp_ts_block.get(u).get(0));
            tmp_tv_cur.add(tmp_ts_block.get(u).get(1));
            tmp_ts_block.set(u + 1, tmp_tv_cur);
        }
        tmp_ts_block.set(j_star, tmp_tv);

        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int max_timestamp = Integer.MIN_VALUE;
        int max_value = Integer.MIN_VALUE;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ts_block.get(j).get(0) - coefficient.get(0);
            float epsilon_v = (float) ts_block.get(j).get(1) - coefficient.get(1);
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0);
                epsilon_v -= coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1);
            }
            ArrayList<Integer> tmp0 = new ArrayList<>();
            tmp0.add((int) epsilon_r);
            tmp0.add((int) epsilon_v);
            ts_block_delta.add(tmp0);
            if (epsilon_r < timestamp_delta_min) {
                timestamp_delta_min = (int) epsilon_r;
            }
            if (epsilon_v < value_delta_min) {
                value_delta_min = (int) epsilon_v;
            }
            if (epsilon_r > max_timestamp) {
                max_timestamp = (int) epsilon_r;
            }
            if (epsilon_v > max_value) {
                max_value = (int) epsilon_v;
            }
        }
        int length = 0;
        for (ArrayList<Integer> integers : ts_block_delta) {
            length += getBitWith(integers.get(0) - timestamp_delta_min);
            length += getBitWith(integers.get(1) - value_delta_min);
        }
        ArrayList<Integer> b = new ArrayList<>();
        b.add(length);

        return b;
    }

    private static ArrayList<Integer> adjustCase2(
            ArrayList<ArrayList<Integer>> ts_block,
            int alpha,
            int j_star,
            ArrayList<Float> coefficient,
            int p) {
        ArrayList<ArrayList<Integer>> tmp_ts_block = (ArrayList<ArrayList<Integer>>) ts_block.clone();
        int block_size = ts_block.size();
        ArrayList<Integer> tmp_tv = tmp_ts_block.get(alpha);
        for (int u = alpha - 1; u >= j_star; u--) {
            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
            tmp_tv_cur.add(tmp_ts_block.get(u).get(0));
            tmp_tv_cur.add(tmp_ts_block.get(u).get(1));
            tmp_ts_block.set(u + 1, tmp_tv_cur);
        }
        tmp_ts_block.set(j_star, tmp_tv);

        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int max_timestamp = Integer.MIN_VALUE;
        int max_value = Integer.MIN_VALUE;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ts_block.get(j).get(0) - coefficient.get(0);
            float epsilon_v = (float) ts_block.get(j).get(1) - coefficient.get(1);
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0);
                epsilon_v -= coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1);
            }
            ArrayList<Integer> tmp0 = new ArrayList<>();
            tmp0.add((int) epsilon_r);
            tmp0.add((int) epsilon_v);
            ts_block_delta.add(tmp0);
            if (epsilon_r < timestamp_delta_min) {
                timestamp_delta_min = (int) epsilon_r;
            }
            if (epsilon_v < value_delta_min) {
                value_delta_min = (int) epsilon_v;
            }
            if (epsilon_r > max_timestamp) {
                max_timestamp = (int) epsilon_r;
            }
            if (epsilon_v > max_value) {
                max_value = (int) epsilon_v;
            }
        }
        int length = 0;
        for (ArrayList<Integer> integers : ts_block_delta) {
            length += getBitWith(integers.get(0) - timestamp_delta_min);
            length += getBitWith(integers.get(1) - value_delta_min);
        }
        ArrayList<Integer> b = new ArrayList<>();
        b.add(length);
        return b;
    }

    private static ArrayList<Integer> adjustCase3(
            ArrayList<ArrayList<Integer>> ts_block,
            int alpha,
            int j_star,
            ArrayList<Float> coefficient,
            int p) {
        ArrayList<ArrayList<Integer>> tmp_ts_block = (ArrayList<ArrayList<Integer>>) ts_block.clone();
        int block_size = ts_block.size();
        ArrayList<Integer> tmp_tv = tmp_ts_block.get(alpha);
        for (int u = alpha + 1; u < j_star; u++) {
            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
            tmp_tv_cur.add(tmp_ts_block.get(u).get(0));
            tmp_tv_cur.add(tmp_ts_block.get(u).get(1));
            tmp_ts_block.set(u - 1, tmp_tv_cur);
        }
        j_star--;
        tmp_ts_block.set(j_star, tmp_tv);

        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int max_timestamp = Integer.MIN_VALUE;
        int max_value = Integer.MIN_VALUE;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ts_block.get(j).get(0) - coefficient.get(0);
            float epsilon_v = (float) ts_block.get(j).get(1) - coefficient.get(1);
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0);
                epsilon_v -= coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1);
            }
            ArrayList<Integer> tmp0 = new ArrayList<>();
            tmp0.add((int) epsilon_r);
            tmp0.add((int) epsilon_v);
            ts_block_delta.add(tmp0);
            if (epsilon_r < timestamp_delta_min) {
                timestamp_delta_min = (int) epsilon_r;
            }
            if (epsilon_v < value_delta_min) {
                value_delta_min = (int) epsilon_v;
            }
            if (epsilon_r > max_timestamp) {
                max_timestamp = (int) epsilon_r;
            }
            if (epsilon_v > max_value) {
                max_value = (int) epsilon_v;
            }
        }
        int length = 0;
        for (ArrayList<Integer> integers : ts_block_delta) {
            length += getBitWith(integers.get(0) - timestamp_delta_min);
            length += getBitWith(integers.get(1) - value_delta_min);
        }
        ArrayList<Integer> b = new ArrayList<>();
        b.add(length);
        return b;
    }

    private static ArrayList<Integer> adjustCase4(
            ArrayList<ArrayList<Integer>> ts_block,
            int alpha,
            int j_star,
            ArrayList<Float> coefficient,
            int p) {
        ArrayList<ArrayList<Integer>> tmp_ts_block = (ArrayList<ArrayList<Integer>>) ts_block.clone();
        int block_size = ts_block.size();
        ArrayList<Integer> tmp_tv = tmp_ts_block.get(alpha);
        for (int u = alpha + 1; u < j_star; u++) {
            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
            tmp_tv_cur.add(tmp_ts_block.get(u).get(0));
            tmp_tv_cur.add(tmp_ts_block.get(u).get(1));
            tmp_ts_block.set(u - 1, tmp_tv_cur);
        }
        j_star--;
        tmp_ts_block.set(j_star, tmp_tv);

        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int max_timestamp = Integer.MIN_VALUE;
        int max_value = Integer.MIN_VALUE;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ts_block.get(j).get(0) - coefficient.get(0);
            float epsilon_v = (float) ts_block.get(j).get(1) - coefficient.get(1);
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0);
                epsilon_v -= coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1);
            }
            ArrayList<Integer> tmp0 = new ArrayList<>();
            tmp0.add((int) epsilon_r);
            tmp0.add((int) epsilon_v);
            ts_block_delta.add(tmp0);
            if (epsilon_r < timestamp_delta_min) {
                timestamp_delta_min = (int) epsilon_r;
            }
            if (epsilon_v < value_delta_min) {
                value_delta_min = (int) epsilon_v;
            }
            if (epsilon_r > max_timestamp) {
                max_timestamp = (int) epsilon_r;
            }
            if (epsilon_v > max_value) {
                max_value = (int) epsilon_v;
            }
        }
        int length = 0;
        for (ArrayList<Integer> integers : ts_block_delta) {
            length += getBitWith(integers.get(0) - timestamp_delta_min);
            length += getBitWith(integers.get(1) - value_delta_min);
        }
        ArrayList<Integer> b = new ArrayList<>();
        b.add(length);
        return b;
    }

    private static ArrayList<Integer> adjustCase5(
            ArrayList<ArrayList<Integer>> ts_block, int alpha, ArrayList<Float> coefficient, int p) {
        ArrayList<ArrayList<Integer>> tmp_ts_block = (ArrayList<ArrayList<Integer>>) ts_block.clone();
        int block_size = ts_block.size();
        ArrayList<Integer> tmp_tv = tmp_ts_block.get(alpha);
        for (int u = alpha + 1; u < block_size; u++) {
            ArrayList<Integer> tmp_tv_cur = new ArrayList<>();
            tmp_tv_cur.add(tmp_ts_block.get(u).get(0));
            tmp_tv_cur.add(tmp_ts_block.get(u).get(1));
            tmp_ts_block.set(u - 1, tmp_tv_cur);
        }
        tmp_ts_block.set(block_size - 1, tmp_tv);

        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int max_timestamp = Integer.MIN_VALUE;
        int max_value = Integer.MIN_VALUE;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();

        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ts_block.get(j).get(0) - coefficient.get(0);
            float epsilon_v = (float) ts_block.get(j).get(1) - coefficient.get(1);
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0);
                epsilon_v -= coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1);
            }
            ArrayList<Integer> tmp0 = new ArrayList<>();
            tmp0.add((int) epsilon_r);
            tmp0.add((int) epsilon_v);
            ts_block_delta.add(tmp0);
            if (epsilon_r < timestamp_delta_min) {
                timestamp_delta_min = (int) epsilon_r;
            }
            if (epsilon_v < value_delta_min) {
                value_delta_min = (int) epsilon_v;
            }
            if (epsilon_r > max_timestamp) {
                max_timestamp = (int) epsilon_r;
            }
            if (epsilon_v > max_value) {
                max_value = (int) epsilon_v;
            }
        }
        int length = 0;
        for (ArrayList<Integer> integers : ts_block_delta) {
            length += getBitWith(integers.get(0) - timestamp_delta_min);
            length += getBitWith(integers.get(1) - value_delta_min);
        }
        ArrayList<Integer> b = new ArrayList<>();
        b.add(length);
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

    public static int getIStarP(
            ArrayList<ArrayList<Integer>> ts_block,
            int block_size,
            int index,
            ArrayList<Float> coefficient,
            int p) {
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int timestamp_delta_max_index = -1;
        int value_delta_max_index = -1;

        int i_star = 0;

        if (index == 0) {
            for (int j = 1; j < p; j++) {
                float epsilon_v_j = (float) ts_block.get(j).get(1) - coefficient.get(1);
                for (int pi = 1; pi <= j; pi++) {
                    epsilon_v_j -= coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1);

                }
                if (epsilon_v_j > value_delta_max) {
                    value_delta_max = (int) epsilon_v_j;
                    value_delta_max_index = j;
                }
            }
            for (int j = p; j < block_size; j++) {
                float epsilon_v_j = (float) ((float) ts_block.get(j).get(1) - coefficient.get(1));
                for (int pi = 1; pi <= p; pi++) {
                    epsilon_v_j -= (float) (coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1));
                }
                if (epsilon_v_j > value_delta_max) {
                    value_delta_max = (int) epsilon_v_j;
                    value_delta_max_index = j;
                }
            }
            //      System.out.println(value_delta_max_index);
            i_star = value_delta_max_index;
        } else if (index == 1) {
            for (int j = 1; j < p; j++) {
                float epsilon_r_j = (float) ((int) ts_block.get(j).get(0) - coefficient.get(0));
                for (int pi = 1; pi <= j; pi++) {
                    epsilon_r_j -= (float) (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));

                }
                if (epsilon_r_j > timestamp_delta_max) {
                    timestamp_delta_max = (int) epsilon_r_j;
                    timestamp_delta_max_index = j;
                }
            }

            for (int j = p; j < block_size; j++) {
                float epsilon_r_j = (float) ((int) ts_block.get(j).get(0) - coefficient.get(0));
                for (int pi = 1; pi <= p; pi++) {
                    epsilon_r_j -= (float) (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));
                }
                if (epsilon_r_j > timestamp_delta_max) {
                    timestamp_delta_max = (int) epsilon_r_j;
                    timestamp_delta_max_index = j;
                }
            }
            i_star = timestamp_delta_max_index;
        }

        return i_star;
    }

    public static ArrayList<Integer> getIStarPK(
            ArrayList<ArrayList<Integer>> ts_block,
            int block_size,
            int index,
            ArrayList<Float> coefficient,
            int p,
            int k) {
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int timestamp_delta_max_index = -1;
        int value_delta_max_index = -1;

        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
        ArrayList<Integer> alpha_list = new ArrayList<>();
        if (index == 0) {
            for (int j = 1; j < p; j++) {
                float epsilon_v_j = (float) ts_block.get(j).get(1) - coefficient.get(1);
                for (int pi = 1; pi <= j; pi++) {
                    epsilon_v_j -= coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1);

                }
                ArrayList<Integer> tmp = new ArrayList<>();
                tmp.add(j);
                tmp.add((int) epsilon_v_j);
                ts_block_delta.add(tmp);
            }
            for (int j = p; j < block_size; j++) {
                float epsilon_v_j = (float) ((float) ts_block.get(j).get(1) - coefficient.get(1));
                for (int pi = 1; pi <= p; pi++) {
                    epsilon_v_j -= (float) (coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1));
                }
                ArrayList<Integer> tmp = new ArrayList<>();
                tmp.add(j);
                tmp.add((int) epsilon_v_j);
                ts_block_delta.add(tmp);
            }

        } else if (index == 1) {
            for (int j = 1; j < p; j++) {
                float epsilon_r_j = (float) ((float) ts_block.get(j).get(0) - coefficient.get(0));
                for (int pi = 1; pi <= j; pi++) {
                    epsilon_r_j -= (float) (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));

                }
                ArrayList<Integer> tmp = new ArrayList<>();
                tmp.add(j);
                tmp.add((int) epsilon_r_j);
                ts_block_delta.add(tmp);
            }

            for (int j = p; j < block_size; j++) {
                float epsilon_r_j = (float) ((float) ts_block.get(j).get(0) - coefficient.get(0));
                for (int pi = 1; pi <= p; pi++) {
                    epsilon_r_j -= (float) (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));
                }
                ArrayList<Integer> tmp = new ArrayList<>();
                tmp.add(j);
                tmp.add((int) epsilon_r_j);
                ts_block_delta.add(tmp);
            }
        }
        quickSort(ts_block_delta, 1, 0, block_size - 2);
        alpha_list.add(ts_block_delta.get(0).get(0));
        for (int i = 0; i < k; i++) {
            alpha_list.add(ts_block_delta.get(block_size - 2 - k).get(0));
        }
        return alpha_list;
    }

    public static ArrayList<Integer> getIStarPK(
            ArrayList<ArrayList<Integer>> ts_block,
            int block_size,
            ArrayList<Integer> raw_length,
            ArrayList<Float> coefficient,
            int p,
            int k) {
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int timestamp_delta_max_index = -1;
        int value_delta_max_index = -1;
        ArrayList<Integer> alpha_list = new ArrayList<>();
        ArrayList<ArrayList<Integer>> ts_block_delta_time = new ArrayList<>();
        ArrayList<ArrayList<Integer>> ts_block_delta_value = new ArrayList<>();

        // regression residual
        for (int j = 1; j < p; j++) {
            float epsilon_r = (float) ((float) ts_block.get(j).get(0) - coefficient.get(0));
            float epsilon_v = (float) ((float) ts_block.get(j).get(1) - coefficient.get(1));
            for (int pi = 1; pi <= j; pi++) {
                epsilon_r -= (float) (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));
                epsilon_v -= (float) (coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1));
            }

            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add(j);
            tmp.add((int) epsilon_r);
            ts_block_delta_time.add(tmp);
            tmp = new ArrayList<>();
            tmp.add(j);
            tmp.add((int) epsilon_v);
            ts_block_delta_value.add(tmp);
        }

        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ((float) ts_block.get(j).get(0) - coefficient.get(0));
            float epsilon_v = (float) ((float) ts_block.get(j).get(1) - coefficient.get(1));
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= (float) (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));
                epsilon_v -= (float) (coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1));
            }

            ArrayList<Integer> tmp = new ArrayList<>();
            tmp.add(j);
            tmp.add((int) epsilon_r);
            ts_block_delta_time.add(tmp);
            tmp = new ArrayList<>();
            tmp.add(j);
            tmp.add((int) epsilon_v);
            ts_block_delta_value.add(tmp);
        }
        quickSort(ts_block_delta_time, 1, 0, block_size - 2);
        alpha_list.add(ts_block_delta_time.get(0).get(0));
        for (int i = 0; i < k; i++) {
            alpha_list.add(ts_block_delta_time.get(block_size - 2 - k).get(0));
        }

        quickSort(ts_block_delta_value, 1, 0, block_size - 2);
        if (!alpha_list.contains(ts_block_delta_value.get(0).get(0)))
            alpha_list.add(ts_block_delta_value.get(0).get(0));
        for (int i = 0; i < k; i++) {
            if (!alpha_list.contains(ts_block_delta_value.get(block_size - 2 - k).get(0)))
                alpha_list.add(ts_block_delta_value.get(block_size - 2 - k).get(0));
        }
        return alpha_list;
    }

    public static int getIStarP(
            ArrayList<ArrayList<Integer>> ts_block,
            int block_size,
            ArrayList<Integer> raw_length,
            ArrayList<Float> coefficient,
            int p) {
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int timestamp_delta_max = Integer.MIN_VALUE;
        int value_delta_max = Integer.MIN_VALUE;
        int timestamp_delta_max_index = -1;
        int value_delta_max_index = -1;

        int i_star = 0;

        // regression residual
        for (int j = 1; j < p; j++) {
            float epsilon_r = (float) ((float) ts_block.get(j).get(0) - coefficient.get(0));
            float epsilon_v = (float) ((float) ts_block.get(j).get(1) - coefficient.get(1));
            for (int pi = 1; pi <= j; pi++) {
                epsilon_r -= (float) (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));
                epsilon_v -= (float) (coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1));
            }

            if (epsilon_r < timestamp_delta_min) {
                timestamp_delta_min = (int) epsilon_r;
            }
            if (epsilon_v < value_delta_min) {
                value_delta_min = (int) epsilon_v;
            }
            if (epsilon_r > timestamp_delta_max) {
                timestamp_delta_max = (int) epsilon_r;
            }
            if (epsilon_v > value_delta_max) {
                value_delta_max = (int) epsilon_v;
            }
        }

        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ((float) ts_block.get(j).get(0) - coefficient.get(0));
            float epsilon_v = (float) ((float) ts_block.get(j).get(1) - coefficient.get(1));
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= (float) (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));
                epsilon_v -= (float) (coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1));
            }

            if (epsilon_r < timestamp_delta_min) {
                timestamp_delta_min = (int) epsilon_r;
            }
            if (epsilon_v < value_delta_min) {
                value_delta_min = (int) epsilon_v;
            }
            if (epsilon_r > timestamp_delta_max) {
                timestamp_delta_max = (int) epsilon_r;
            }
            if (epsilon_v > value_delta_max) {
                value_delta_max = (int) epsilon_v;
            }
        }
        timestamp_delta_max -= timestamp_delta_min;
        value_delta_max -= value_delta_min;
        if (value_delta_max <= timestamp_delta_max) i_star = timestamp_delta_max_index;
        else i_star = value_delta_max_index;
        return i_star;
    }

    public static ArrayList<Byte> encode2Bytes(
            ArrayList<ArrayList<Integer>> ts_block_delta,
            ArrayList<Integer> raw_length,
            ArrayList<Float> coefficient,
            ArrayList<Integer> result2,
            int p) {
        ArrayList<Byte> encoded_result = new ArrayList<>();

        // encode interval0 and value0
        for (int i = 0; i < p; i++) {
            byte[] interval0_byte = int2Bytes(ts_block_delta.get(i).get(0));
            for (byte b : interval0_byte) encoded_result.add(b);
            byte[] value0_byte = int2Bytes(ts_block_delta.get(i).get(1));
            for (byte b : value0_byte) encoded_result.add(b);
        }
        // encode theta
        byte[] theta0_r_byte = float2bytes(coefficient.get(0) + (float) raw_length.get(3));
        for (byte b : theta0_r_byte) encoded_result.add(b);
        byte[] theta0_v_byte = float2bytes(coefficient.get(1) + (float) raw_length.get(4));
        for (byte b : theta0_v_byte) encoded_result.add(b);


        for (int i = 2; i < coefficient.size(); i++) {
            byte[] theta_byte = float2bytes(coefficient.get(i));
            for (byte b : theta_byte) encoded_result.add(b);
        }

        byte[] max_bit_width_interval_byte = bitWidth2Bytes(raw_length.get(1));
        for (byte b : max_bit_width_interval_byte) encoded_result.add(b);
        byte[] timestamp_bytes = bitPacking(ts_block_delta, 0, p, ts_block_delta.size() - p, raw_length.get(1));
        for (byte b : timestamp_bytes) encoded_result.add(b);


        // encode value
        byte[] max_bit_width_value_byte = bitWidth2Bytes(raw_length.get(2));
        for (byte b : max_bit_width_value_byte) encoded_result.add(b);
        byte[] value_bytes = bitPacking(ts_block_delta, 1, p, ts_block_delta.size() - p, raw_length.get(2));
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
            if (cur_time != pre_time) { // 当前值与前一个值不同
                ArrayList<Integer> tmp = new ArrayList<>();
                tmp.add(count_of_time);
                tmp.add(pre_time);
                run_length_time.add(tmp);
                pre_time = cur_time;
                count_of_time = 0;
            } else {// 当前值与前一个值相同
                count_of_time++;
                if (count_of_time == 256) { // 个数不能大于256
                    ArrayList<Integer> tmp = new ArrayList<>();
                    tmp.add(count_of_time);
                    tmp.add(pre_time);
                    run_length_time.add(tmp);
                    count_of_time = 0;
                }
            }

            if (cur_value != pre_value) { // 当前值与前一个值不同
                ArrayList<Integer> tmp = new ArrayList<>();
                tmp.add(count_of_value);
                tmp.add(pre_value);
                run_length_value.add(tmp);
                pre_value = cur_value;
                count_of_value = 0;
            } else {// 当前值与前一个值相同
                count_of_value++;
                if (count_of_value == 256) { // 个数不能大于256
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
            ArrayList<ArrayList<Integer>> data, int block_size, int[] third_value, int segment_size, int p, int k) {
        for (int i = 0; i < p; i++)
            block_size++;
        ArrayList<Byte> encoded_result = new ArrayList<Byte>();
        int length_all = data.size();
        byte[] length_all_bytes = int2Bytes(length_all);
        for (byte b : length_all_bytes) encoded_result.add(b);
        int block_num = length_all / block_size;

        // encode block size (Integer)
        byte[] block_size_byte = int2Bytes(block_size);
        for (byte b : block_size_byte) encoded_result.add(b);

        byte[] p_byte = bitWidth2Bytes(p);
        for (byte b : p_byte) encoded_result.add(b);

        // ----------------------- compare data order by time, value and partition ---------------------------
        int length_time = 0;
        int length_value = 0;
        int length_partition = 0;
        ArrayList<ArrayList<Integer>> data_value = (ArrayList<ArrayList<Integer>>) data.clone();
        quickSort(data_value, 1, 0, length_all - 1);

        ArrayList<ArrayList<Integer>> data_partition = new ArrayList<>();

        for (ArrayList<Integer> datum : data) {
            if (datum.get(1) > third_value[third_value.length - 1]) {
                data_partition.add(datum);
            }
        }
        for (int third_i = third_value.length - 1; third_i > 0; third_i--) {
            for (ArrayList<Integer> datum : data) {
                if (datum.get(1) <= third_value[third_i] && datum.get(1) > third_value[third_i - 1]) {
                    data_partition.add(datum);
                }
            }
        }
        for (ArrayList<Integer> datum : data) {
            if (datum.get(1) <= third_value[0]) {
                data_partition.add(datum);
            }
        }
        for (int i = 0; i < block_num; i++) {
            ArrayList<ArrayList<Integer>> ts_block_time = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_value = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_partition = new ArrayList<>();

            for (int j = 0; j < block_size; j++) {
                ts_block_time.add(data.get(j + i * block_size));
                ts_block_value.add(data_value.get(j + i * block_size));
                ts_block_partition.add(data_partition.get(j + i * block_size));
            }
            ArrayList<Integer> result1 = new ArrayList<>();
            splitTimeStamp3(ts_block_time, result1);
            ArrayList<Integer> raw_length = new ArrayList<>();
            ArrayList<Float> coefficient = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegressionP(ts_block_time, block_size, raw_length, coefficient, p);
            ArrayList<ArrayList<Integer>> bit_width_segments = segmentBitPacking(ts_block_delta, block_size, segment_size);
            length_time += encodeSegment2Bytes(ts_block_delta, bit_width_segments, raw_length, segment_size, coefficient, result1, p).size();

            ArrayList<Integer> result2 = new ArrayList<>();
            splitTimeStamp3(ts_block_value, result2);
            ArrayList<Integer> raw_length_value = new ArrayList<>();
            ArrayList<Float> coefficient_value = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta_value = getEncodeBitsRegressionP(ts_block_value, block_size, raw_length_value, coefficient_value, p);
            ArrayList<ArrayList<Integer>> bit_width_segments_value = segmentBitPacking(ts_block_delta_value, block_size, segment_size);
            length_value += encodeSegment2Bytes(ts_block_delta_value, bit_width_segments_value, raw_length_value, segment_size, coefficient_value, result2, p).size();

            ArrayList<Integer> result3 = new ArrayList<>();
            splitTimeStamp3(ts_block_partition, result3);
            ArrayList<Integer> raw_length_partition = new ArrayList<>();
            ArrayList<Float> coefficient_partition = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta_partition = getEncodeBitsRegressionP(ts_block_partition, block_size, raw_length_partition, coefficient_partition, p);
            ArrayList<ArrayList<Integer>> bit_width_segments_partition = segmentBitPacking(ts_block_delta_partition, block_size, segment_size);
            length_partition += encodeSegment2Bytes(ts_block_delta_partition, bit_width_segments_partition, raw_length_partition, segment_size, coefficient_partition, result3, p).size();

        }
        if (length_partition < length_time && length_partition < length_value) { // partition performs better
            data = data_partition;

            for (int i = 0; i < block_num; i++) {
                ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();
                for (int j = 0; j < block_size; j++) {
                    ts_block.add(data.get(j + i * block_size));
                    ts_block_reorder.add(data.get(j + i * block_size));
                }

                ArrayList<Integer> result2 = new ArrayList<>();

                splitTimeStamp3(ts_block, result2);
                splitTimeStamp3(ts_block_reorder, result2);

                ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
                ArrayList<Float> coefficient = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta =
                        getEncodeBitsRegressionP(ts_block, block_size, raw_length, coefficient, p);
                ArrayList<ArrayList<Integer>> bit_width_segments_partition = segmentBitPacking(ts_block_delta, block_size, segment_size);
                raw_length.set(0, encodeSegment2Bytes(ts_block_delta, bit_width_segments_partition, raw_length, segment_size, coefficient, result2, p).size());


                // time-order
                quickSort(ts_block, 0, 0, block_size - 1);
                ArrayList<Integer> time_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
                ArrayList<Float> coefficient_time = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta_time =
                        getEncodeBitsRegressionP(ts_block, block_size, time_length, coefficient_time, p);
                ArrayList<ArrayList<Integer>> bit_width_segments_time = segmentBitPacking(ts_block_delta_time, block_size, segment_size);
                time_length.set(0, encodeSegment2Bytes(ts_block_delta_time, bit_width_segments_time, time_length, segment_size, coefficient_time, result2, p).size());

                // value-order
                quickSort(ts_block, 1, 0, block_size - 1);


                ArrayList<Integer> reorder_length = new ArrayList<>();
                ArrayList<Float> coefficient_reorder = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta_reorder =
                        getEncodeBitsRegressionP(ts_block, block_size, reorder_length, coefficient_reorder, p);
                ArrayList<ArrayList<Integer>> bit_width_segments_value = segmentBitPacking(ts_block_delta_reorder, block_size, segment_size);
                reorder_length.set(0, encodeSegment2Bytes(ts_block_delta_reorder, bit_width_segments_value, reorder_length, segment_size, coefficient_reorder, result2, p).size());


                ArrayList<Integer> alpha_list = new ArrayList<>();
                int choose = min3(time_length.get(0), raw_length.get(0), reorder_length.get(0));
                if (choose == 0) {
                    raw_length = time_length;
                    quickSort(ts_block, 0, 0, block_size - 1);
                    coefficient = coefficient_time;
                    ts_block_delta = ts_block_delta_time;
                    alpha_list = getIStarPK(ts_block, block_size, 0, coefficient, p, k);
                } else if (choose == 1) {
                    ts_block = ts_block_reorder;
                    alpha_list = getIStarPK(ts_block, block_size, 0, coefficient, p, k);
                } else {
                    raw_length = reorder_length;
                    coefficient = coefficient_reorder;
                    ts_block_delta = ts_block_delta_reorder;
                    alpha_list = getIStarPK(ts_block, block_size, 1, coefficient, p, k);
                }
                ArrayList<Integer> beta_list;
                beta_list = new ArrayList<>();
//                System.out.println("coefficient: "+coefficient);
                for (int alpha : alpha_list) {
                    beta_list.add(getBetaP(ts_block, alpha, block_size, raw_length, coefficient, p));
                }
                ArrayList<Integer> isMoveable = isMovable(alpha_list, beta_list);
                int adjust_count = 0;
                while (isMoveable.size() != 0) {
                    if (adjust_count < block_size / 2 && adjust_count <= 20) {
                        adjust_count++;
                    } else {
                        break;
                    }
                    ArrayList<ArrayList<Integer>> all_length = new ArrayList<>();
//                        System.out.println("theta: "+theta);

                    for (int isMoveable_i : isMoveable) {
                        ArrayList<ArrayList<Integer>> new_ts_block = (ArrayList<ArrayList<Integer>>) ts_block.clone();
                        ArrayList<Integer> new_length = new ArrayList<>();
                        moveAlphaToBeta(new_ts_block, alpha_list.get(isMoveable_i), beta_list.get(isMoveable_i));
                        getEncodeBitsRegressionPNoTrain(new_ts_block, block_size, new_length, coefficient, p);
                        ArrayList<Integer> tmp = new ArrayList<>();
                        tmp.add(isMoveable_i);
                        tmp.add(new_length.get(0));
                        all_length.add(tmp);

                    }

                    quickSort(all_length, 1, 0, all_length.size() - 1);
                    if (all_length.get(0).get(1) <= raw_length.get(0)) {
                        moveAlphaToBeta(ts_block, alpha_list.get(all_length.get(0).get(0)), beta_list.get(all_length.get(0).get(0)));
                        getEncodeBitsRegressionPNoTrain(ts_block, block_size, raw_length, coefficient, p);
                    } else {
                        break;
                    }
                    alpha_list = getIStarPK(ts_block, block_size, raw_length, coefficient, p, k);
                    int alpha_size = alpha_list.size();
                    for (int alpha_i = alpha_size - 1; alpha_i >= 0; alpha_i--) {
                        if (beta_list.contains(alpha_list.get(alpha_i))) {
                            alpha_list.remove(alpha_i);
                        }
                    }
                    beta_list = new ArrayList<>();
                    for (int alpha : alpha_list) {
                        beta_list.add(getBetaP(ts_block, alpha, block_size, raw_length, coefficient, p));
                    }
                    isMoveable = isMovable(alpha_list, beta_list);
                }

                ts_block_delta = getEncodeBitsRegressionPNoTrain(ts_block, block_size, raw_length, coefficient, p);
                ArrayList<ArrayList<Integer>> bit_width_segments = new ArrayList<>();
                int segment_n = (block_size - p) / segment_size;
                for (int segment_i = 0; segment_i < segment_n; segment_i++) {
                    int bit_width_time = Integer.MIN_VALUE;
                    int bit_width_value = Integer.MIN_VALUE;

                    for (int data_i = segment_i * segment_size + p; data_i < (segment_i + 1) * segment_size + p; data_i++) {
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


                ArrayList<Byte> cur_encoded_result = encodeSegment2Bytes(ts_block_delta, bit_width_segments, raw_length, segment_size, coefficient, result2, p);
                encoded_result.addAll(cur_encoded_result);


            }
        } else {
            if (length_value < length_time) { // order by value performs better

                data = data_value;
            }
            for (int i = 0; i < block_num; i++) {
                ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

                ArrayList<ArrayList<Integer>> ts_block_partition = new ArrayList<>();
                for (int j = 0; j < block_size; j++) {
                    ts_block.add(data.get(j + i * block_size));

                }

                ArrayList<Integer> result2 = new ArrayList<>();

                splitTimeStamp3(ts_block, result2);

                quickSort(ts_block, 0, 0, block_size - 1);
                ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
                ArrayList<Float> coefficient = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegressionP(ts_block, block_size, raw_length, coefficient, p);
                ArrayList<ArrayList<Integer>> bit_width_segments_time = segmentBitPacking(ts_block_delta, block_size, segment_size);
                raw_length.set(0, encodeSegment2Bytes(ts_block_delta, bit_width_segments_time, raw_length, segment_size, coefficient, result2, p).size());

                // value-order
                quickSort(ts_block, 1, 0, block_size - 1);

                ArrayList<Integer> reorder_length = new ArrayList<>();
                ArrayList<Float> coefficient_reorder = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta_reorder = getEncodeBitsRegressionP(ts_block, block_size, reorder_length, coefficient_reorder, p);
                ArrayList<ArrayList<Integer>> bit_width_segments_value = segmentBitPacking(ts_block_delta_reorder, block_size, segment_size);
                reorder_length.set(0, encodeSegment2Bytes(ts_block_delta_reorder, bit_width_segments_value, reorder_length, segment_size, coefficient_reorder, result2, p).size());

                for (ArrayList<Integer> datum : ts_block) {
                    if (datum.get(1) > third_value[third_value.length - 1]) {
                        ts_block_partition.add(datum);
                    }
                }
                for (int third_i = third_value.length - 1; third_i > 0; third_i--) {
                    for (ArrayList<Integer> datum : ts_block) {
                        if (datum.get(1) <= third_value[third_i] && datum.get(1) > third_value[third_i - 1]) {
                            ts_block_partition.add(datum);
                        }
                    }
                }
                for (ArrayList<Integer> datum : ts_block) {
                    if (datum.get(1) <= third_value[0]) {
                        ts_block_partition.add(datum);
                    }
                }
                ArrayList<Integer> partition_length = new ArrayList<>();
                ArrayList<Float> coefficient_partition = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta_partition = getEncodeBitsRegressionP(ts_block_partition, block_size, partition_length, coefficient_partition, p);
                ArrayList<ArrayList<Integer>> bit_width_segments_partition = segmentBitPacking(ts_block_delta_partition, block_size, segment_size);
                partition_length.set(0, encodeSegment2Bytes(ts_block_delta_partition, bit_width_segments_partition, partition_length, segment_size, coefficient_partition, result2, p).size());


                int choose = min3(partition_length.get(0), reorder_length.get(0), raw_length.get(0));
                ArrayList<Integer> alpha_list = new ArrayList<>();
                if (choose == 0) {
                    raw_length = partition_length;
                    ts_block = ts_block_partition;
                    ts_block_delta = ts_block_delta_partition;
                    coefficient = coefficient_partition;
                    alpha_list = getIStarPK(ts_block, block_size, 0, coefficient, p, k);
                } else if (choose == 1) {
                    raw_length = reorder_length;
                    quickSort(ts_block, 1, 0, block_size - 1);
                    ts_block_delta = ts_block_delta_reorder;
                    coefficient = coefficient_reorder;
                    alpha_list = getIStarPK(ts_block, block_size, 1, coefficient, p, k);
                } else {
//                    ts_block = ts_block_reorder;
                    quickSort(ts_block, 0, 0, block_size - 1);
                    alpha_list = getIStarPK(ts_block, block_size, 0, coefficient, p, k);
                }
                ArrayList<Integer> beta_list;
                beta_list = new ArrayList<>();
                for (int alpha : alpha_list) {
                    beta_list.add(getBetaP(ts_block, alpha, block_size, raw_length, coefficient, p));
                }
                ArrayList<Integer> isMoveable = isMovable(alpha_list, beta_list);
                int adjust_count = 0;
                while (isMoveable.size() != 0) {
                    if (adjust_count < block_size / 2 && adjust_count <= 20) {
                        adjust_count++;
                    } else {
                        break;
                    }
                    ArrayList<ArrayList<Integer>> all_length = new ArrayList<>();


                    for (int isMoveable_i : isMoveable) {
                        ArrayList<ArrayList<Integer>> new_ts_block = (ArrayList<ArrayList<Integer>>) ts_block.clone();
                        ArrayList<Integer> new_length = new ArrayList<>();
                        moveAlphaToBeta(new_ts_block, alpha_list.get(isMoveable_i), beta_list.get(isMoveable_i));
                        getEncodeBitsRegressionPNoTrain(new_ts_block, block_size, new_length, coefficient, p);
                        ArrayList<Integer> tmp = new ArrayList<>();
                        tmp.add(isMoveable_i);
                        tmp.add(new_length.get(0));
                        all_length.add(tmp);

                    }

                    quickSort(all_length, 1, 0, all_length.size() - 1);
                    if (all_length.get(0).get(1) <= raw_length.get(0)) {
                        moveAlphaToBeta(ts_block, alpha_list.get(all_length.get(0).get(0)), beta_list.get(all_length.get(0).get(0)));

                        getEncodeBitsRegressionPNoTrain(ts_block, block_size, raw_length, coefficient, p);
                    } else {
                        break;
                    }
                    alpha_list = getIStarPK(ts_block, block_size, raw_length, coefficient, p, k);
                    int alpha_size = alpha_list.size();
                    for (int alpha_i = alpha_size - 1; alpha_i >= 0; alpha_i--) {
                        if (beta_list.contains(alpha_list.get(alpha_i))) {
                            alpha_list.remove(alpha_i);
                        }
                    }
                    beta_list = new ArrayList<>();
                    for (int alpha : alpha_list) {
                        beta_list.add(getBetaP(ts_block, alpha, block_size, raw_length, coefficient, p));
                    }
                    isMoveable = isMovable(alpha_list, beta_list);
                }


                ts_block_delta = getEncodeBitsRegressionPNoTrain(ts_block, block_size, raw_length, coefficient, p);

                ArrayList<ArrayList<Integer>> bit_width_segments = new ArrayList<>();
                int segment_n = (block_size - p) / segment_size;
                for (int segment_i = 0; segment_i < segment_n; segment_i++) {
                    int bit_width_time = Integer.MIN_VALUE;
                    int bit_width_value = Integer.MIN_VALUE;

                    for (int data_i = segment_i * segment_size + p; data_i < (segment_i + 1) * segment_size + p; data_i++) {
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


                ArrayList<Byte> cur_encoded_result = encodeSegment2Bytes(ts_block_delta, bit_width_segments, raw_length, segment_size, coefficient, result2, p);
                encoded_result.addAll(cur_encoded_result);

            }
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
            ArrayList<Integer> i_star_ready = new ArrayList<>();
            ArrayList<Float> coefficient = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta =
                    getEncodeBitsRegressionP(ts_block, remaining_length, raw_length, coefficient, p);

            // value-order
            quickSort(ts_block, 1, 0, remaining_length - 1);
            ArrayList<Integer> reorder_length = new ArrayList<>();
            ArrayList<Integer> i_star_ready_reorder = new ArrayList<>();
            ArrayList<Float> coefficient_reorder = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta_reorder =
                    getEncodeBitsRegressionP(
                            ts_block, remaining_length, reorder_length, coefficient_reorder, p);

            if (raw_length.get(0) <= reorder_length.get(0)) {
                quickSort(ts_block, 0, 0, remaining_length - 1);
            } else {
                raw_length = reorder_length;
                coefficient = coefficient_reorder;
                quickSort(ts_block, 1, 0, remaining_length - 1);
            }
            ts_block_delta =
                    getEncodeBitsRegressionP(ts_block, remaining_length, raw_length, coefficient, p);
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
            ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta, raw_length, coefficient, result2, p);

            encoded_result.addAll(cur_encoded_result);
        }

        return encoded_result;
    }

    private static ArrayList<Byte> encodeSegment2Bytes(ArrayList<ArrayList<Integer>> delta_segments,
                                                       ArrayList<ArrayList<Integer>> bit_width_segments,
                                                       ArrayList<Integer> raw_length, int segment_size, ArrayList<Float> coefficient, ArrayList<Integer> result2, int p) {
        ArrayList<Byte> encoded_result = new ArrayList<>();
        int block_size = delta_segments.size();
        int segment_n = (block_size - p) / segment_size;
        // encode theta


        // encode interval0 and value0
        for (int i = 0; i < p; i++) {
            byte[] interval0_byte = int2Bytes(delta_segments.get(i).get(0));
            for (byte b : interval0_byte) encoded_result.add(b);
            byte[] value0_byte = int2Bytes(delta_segments.get(i).get(1));
            for (byte b : value0_byte) encoded_result.add(b);
        }

        // encode theta
        byte[] theta0_r_byte = float2bytes(coefficient.get(0) + (float) raw_length.get(3));
        for (byte b : theta0_r_byte) encoded_result.add(b);
        byte[] theta0_v_byte = float2bytes(coefficient.get(1) + (float) raw_length.get(4));
        for (byte b : theta0_v_byte) encoded_result.add(b);


        for (int i = 2; i < coefficient.size(); i++) {
            byte[] theta_byte = float2bytes(coefficient.get(i));
            for (byte b : theta_byte) encoded_result.add(b);
        }

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

    public static float byte2float2(ArrayList<Byte> b, int index) {
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

    public static ArrayList<ArrayList<Integer>> ReorderingRegressionDecoder(
            ArrayList<Byte> encoded, int td) {
        ArrayList<ArrayList<Integer>> data = new ArrayList<>();
        int decode_pos = 0;
        int block_size = bytes2Integer(encoded, decode_pos, 4);
        decode_pos += 4;

        while (decode_pos < encoded.size()) {
            ArrayList<Integer> time_list = new ArrayList<>();
            ArrayList<Integer> value_list = new ArrayList<>();
            // ArrayList<Integer> deviation_list = new ArrayList<>();

            ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();

            int time0 = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;
            int value0 = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;

            float theta0_r = byte2float2(encoded, decode_pos);
            decode_pos += 4;
            float theta1_r = byte2float2(encoded, decode_pos);
            decode_pos += 4;
            float theta0_v = byte2float2(encoded, decode_pos);
            decode_pos += 4;
            float theta1_v = byte2float2(encoded, decode_pos);
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
        return data;
    }

    public static double[] dataStandardization(double array[]) {
        StandardDeviation deviation = new StandardDeviation();
        for (int i = 0; i < array.length; i++) {
            array[i] = array[i];
        }
        return array;
    }

    public static double[][] dataStandardizationDouble(double arrays[][]) {
        double[][] result = new double[arrays[0].length][arrays.length];
        for (int i = 0; i < arrays.length; i++) {
            double[] doubles = dataStandardization(arrays[i]);
            for (int k = 0; k < result.length; k++) {
                result[k][i] = doubles[k];
            }
        }
        return result;
    }


    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {

        String parent_dir = "C:\\Users\\Jinnsjao Shawl\\Documents\\GitHub\\encoding-reorder\\";
        String output_parent_dir = parent_dir + "vldb\\compression_ratio\\p_float_test";
        String input_parent_dir = parent_dir + "reorder\\iotdb_test_small\\";

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

        output_path_list.add(output_parent_dir + "\\CS-Sensors_ratio.csv"); // 0
        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "\\Metro-Traffic_ratio.csv");// 1
        dataset_block_size.add(512);
        output_path_list.add(output_parent_dir + "\\USGS-Earthquakes_ratio.csv");// 2
        dataset_block_size.add(512);
        output_path_list.add(output_parent_dir + "\\YZ-Electricity_ratio.csv"); // 3
        dataset_block_size.add(1024);
        output_path_list.add(output_parent_dir + "\\GW-Magnetic_ratio.csv"); //4
        dataset_block_size.add(128);
        output_path_list.add(output_parent_dir + "\\TY-Fuel_ratio.csv");//5
        dataset_block_size.add(64);
        output_path_list.add(output_parent_dir + "\\Cyber-Vehicle_ratio.csv"); //6
        dataset_block_size.add(128);
        output_path_list.add(output_parent_dir + "\\Vehicle-Charge_ratio.csv");//7
        dataset_block_size.add(512);
        output_path_list.add(output_parent_dir + "\\Nifty-Stocks_ratio.csv");//8
        dataset_block_size.add(256);
        output_path_list.add(output_parent_dir + "\\TH-Climate_ratio.csv");//9
        dataset_block_size.add(512);
        output_path_list.add(output_parent_dir + "\\TY-Transport_ratio.csv");//10
        dataset_block_size.add(512);
        output_path_list.add(output_parent_dir + "\\EPM-Education_ratio.csv");//11
        dataset_block_size.add(512);


        for (int file_i = 3; file_i < 4; file_i++) {

            String inputPath = input_path_list.get(file_i);
            String Output = output_path_list.get(file_i);

            // speed
            int repeatTime = 1; // set repeat time

            File file = new File(inputPath);
            File[] tempList = file.listFiles();

            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    //      "Compress Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    //      "Compress Time",
                    //      "Uncompress Time",
                    "Points",
                    "p",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file

            assert tempList != null;

            for (File f : tempList) {

                for (int p = 1; p < 10; p++) {
                    System.out.println("p=" + p);



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

                    for (int i = 0; i < repeatTime; i++) {
                        long s = System.nanoTime();
                        ArrayList<Byte> buffer = new ArrayList<>();
                        for (int repeat_i = 0; repeat_i < 1; repeat_i++)
                            buffer = ReorderingRegressionEncoder(data, dataset_block_size.get(file_i), dataset_third.get(file_i), 8, p, 3);


                        long e = System.nanoTime();
                        encodeTime += ((e - s));
                        compressed_size += buffer.size();
                        double ratioTmp = (double) buffer.size() / (double) (data.size() * Integer.BYTES * 2);
                        ratio += ratioTmp;
                        s = System.nanoTime();
                        e = System.nanoTime();
                        decodeTime += ((e - s));


                    }

                    ratio /= repeatTime;
                    compressed_size /= repeatTime;
                    encodeTime /= repeatTime;
                    decodeTime /= repeatTime;

                    String[] record = {
                            f.toString(),
                            "REGER-32-FLOAT",
                            String.valueOf(encodeTime),
                            String.valueOf(decodeTime),
                            String.valueOf(data.size()),
                            String.valueOf(p),
                            String.valueOf(compressed_size),
                            String.valueOf(ratio)
                    };

                    writer.writeRecord(record);

                }
            }
            writer.close();
        }
    }

}
