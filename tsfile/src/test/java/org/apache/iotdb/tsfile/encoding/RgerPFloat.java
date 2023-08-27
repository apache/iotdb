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

public class RgerPFloat {

    public static int zigzag(int num) {
        if (num < 0) {
            return 2 * (-num) - 1;
        } else {
            return 2 * num;
        }
    }

    public static int min3(int a, int b, int c) {
        if (a < b && a < c) {
            return 0;
        } else if (b < c ) {
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

    public static byte[] double2Bytes(double dou) {
        long value = Double.doubleToRawLongBits(dou);
        byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) ((value >> 8 * i) & 0xff);
        }
        return bytes;
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
                //             System.out.println(Integer.toBinaryString(tmp_int));
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
                //        System.out.println(Integer.toBinaryString(tmp_int));
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
            //      while (low < high && (arr.get(high).get(index) >= tmp.get(index))) {
            //        high--;
            //      }
            //      arr.set(low,arr.get(high));
            //      while (low < high && (arr.get(low).get(index) <= tmp.get(index))) {
            //        low++;
            //      }
            arr.set(high, arr.get(low));
        }
        arr.set(low, tmp);
        return low;
    }

    public static void quickSort(ArrayList<ArrayList<Integer>> arr, int index, int low, int high) {
        Stack<Integer> stack = new Stack<>();
        int mid = part(arr, index, low, high);
        // 判断右半部分是否仅有一个数据
        // 将边界入栈，需要注意左右部分都先压左边界或右边界。顺序需要相同，以防出栈时不好判断是low还是high，此方法先压左边界后压右边界
        if (mid + 1 < high) {
            stack.push(mid + 1);
            stack.push(high);
        }
        // 判断左半部分是否仅有一个数据
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
        // int max_deviation = Integer.MIN_VALUE;

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

//    td = td_common;

        int t0 = ts_block.get(0).get(0);
        for (int i = 0; i < ts_block.size(); i++) {
            ArrayList<Integer> tmp = new ArrayList<>();
            int interval_i = (ts_block.get(i).get(0) - t0) / td_common;
            // int deviation_i = ts_block.get(i).get(0) - t0 - interval_i * td;
            //      tmp.add(t0+interval_i);
            tmp.add(interval_i);
            tmp.add(ts_block.get(i).get(1));
            ts_block.set(i, tmp);

            // deviation_list.add(zigzag(deviation_i));
            // if(zigzag(deviation_i)>max_deviation){
            //  max_deviation = zigzag(deviation_i);
            // }
        }
        ArrayList<Integer> tmp = new ArrayList<>();
        // int deviation_i = ts_block.get(i).get(0) - t0 - interval_i * td;
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

        double[] resultCovariances_value = new double[p + 1];
        double[] resultCovariances_timestamp = new double[p + 1];
        for (int i = 0; i <= p; i++) {
            resultCovariances_value[i] = 0;
            resultCovariances_timestamp[i] = 0;
            for (int j = 0; j < length - i; j++) {
                if (j + i < length) {
                    resultCovariances_timestamp[i] += ts_block.get(j).get(0) * ts_block.get(j + i).get(0);
                    resultCovariances_value[i] += ts_block.get(j).get(1) * ts_block.get(j + i).get(1);
                }
            }
            resultCovariances_timestamp[i] /= length - i;
            resultCovariances_value[i] /= length - i;
        }

        double[] epsilons_timestamp = new double[p + 1];
        double[] epsilons_value = new double[p + 1];
        double[] kappas_timestamp = new double[p + 1];
        double[] kappas_value = new double[p + 1];
        double[][] alphas_timestamp = new double[p + 1][p + 1];
        double[][] alphas_value = new double[p + 1][p + 1];
        // alphas_timestamp[i][j] denotes alpha_i^{(j)}
        // alphas_value[i][j] denotes alpha_i^{(j)}
        epsilons_timestamp[0] = resultCovariances_timestamp[0];
        epsilons_value[0] = resultCovariances_value[0];
        for (int i = 1; i <= p; i++) {
            double tmpSum_timestamp = 0.0;
            double tmpSum_value = 0.0;
            for (int j = 1; j <= i - 1; j++) {
                tmpSum_timestamp += alphas_timestamp[j][i - 1] * resultCovariances_timestamp[i - j];
                tmpSum_value += alphas_value[j][i - 1] * resultCovariances_value[i - j];
            }
            kappas_timestamp[i] =
                    (resultCovariances_timestamp[i] - tmpSum_timestamp) / epsilons_timestamp[i - 1];
            kappas_value[i] = (resultCovariances_value[i] - tmpSum_value) / epsilons_value[i - 1];
            alphas_timestamp[i][i] = kappas_timestamp[i];
            alphas_value[i][i] = kappas_value[i];
            if (i > 1) {
                for (int j = 1; j <= i - 1; j++) {
                    alphas_timestamp[j][i] =
                            alphas_timestamp[j][i - 1] - kappas_timestamp[i] * alphas_timestamp[i - j][i - 1];
                    alphas_value[j][i] =
                            alphas_value[j][i - 1] - kappas_value[i] * alphas_value[i - j][i - 1];
                }
            }
            epsilons_timestamp[i] =
                    (1 - kappas_timestamp[i] * kappas_timestamp[i]) * epsilons_timestamp[i - 1];
            epsilons_value[i] = (1 - kappas_value[i] * kappas_value[i]) * epsilons_value[i - 1];
        }

        for (int i = 0; i <= p; i++) {
            coefficient.add((float) alphas_timestamp[i][p]);
            coefficient.add((float) alphas_value[i][p]);
            //      System.out.println(alphas_value[i][3]);
        }
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

            //      if(epsilon_r<timestamp_delta_min){
            //        timestamp_delta_min = epsilon_r;
            //      }
            //      if(epsilon_v<value_delta_min){
            //        value_delta_min = epsilon_v;
            //      }
            //      if(epsilon_r>max_timestamp){
            //        max_timestamp = epsilon_r;
            //      }
            //      if(epsilon_v>max_value){
            //        max_value = epsilon_v;
            //      }
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
        int length = 0 ;
        for (int j = block_size - 1; j >= p; j--) {
            float epsilon_r = ts_block_delta.get(j).get(0) - timestamp_delta_min;
            float epsilon_v = ts_block_delta.get(j).get(1) - value_delta_min;
            length += epsilon_r;
            length += epsilon_v;
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

    public static int getBetaP(
            ArrayList<ArrayList<Integer>> ts_block,
            int alpha,
            int block_size,
            ArrayList<Float> coefficient,
            int p) {
        int timestamp_delta_min = Integer.MAX_VALUE;
        int value_delta_min = Integer.MAX_VALUE;
        int max_timestamp = Integer.MIN_VALUE;
        int max_value = Integer.MIN_VALUE;
        int raw_timestamp_delta_max_index = -1;
        int raw_value_delta_max_index = -1;

        int raw_abs_sum = 0;
        ArrayList<ArrayList<Integer>> ts_block_delta = new ArrayList<>();
        coefficient.clear();


        ArrayList<Integer> j_star_list = new ArrayList<>(); // beta list of min b phi alpha to j
        ArrayList<Integer> max_index = new ArrayList<>();
        int j_star = -1;

        if (alpha == -1) {
            return j_star;
        }
        terminate(ts_block, coefficient, p);
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ts_block.get(j).get(0) - coefficient.get(0);
            float epsilon_v = (float) ts_block.get(j).get(1) - coefficient.get(1);
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
        for (int j = 0; j < block_size - p; j++) {
            raw_abs_sum += ts_block_delta.get(j).get(0);
            raw_abs_sum += ts_block_delta.get(j).get(1);
        }
        // regression residual
        for (int j = p; j < block_size; j++) {
            float epsilon_r = (float) ts_block.get(j).get(0) - coefficient.get(0);
            float epsilon_v = (float) ts_block.get(j).get(1) - coefficient.get(1);
            for (int pi = 1; pi <= p; pi++) {
                epsilon_r -= (coefficient.get(2 * pi) * (float) ts_block.get(j - pi).get(0));
                epsilon_v -= (coefficient.get(2 * pi + 1) * (float) ts_block.get(j - pi).get(1));
            }
            if (j != alpha && ((int) epsilon_r == max_timestamp || (int) epsilon_v == max_value)) {
                max_index.add(j);
            }
        }
        //    System.out.println(raw_length);
        //    System.out.println(raw_bit_width_timestamp);
        //    System.out.println(raw_bit_width_value);
        // alpha <= p
        if (alpha < p) {
            //      System.out.println("alpha == 1");
            int j = 0;
            for (; j < alpha; j++) {
                // judge whether j is in hj and gj,  [j, alpha-1] and [alpha + 1, min{j+p,n}], [j + p + 1,
                // min{alpha+p,n}]
                boolean is_contain_2p = false;
                for (int hg_j = j; hg_j <= alpha + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
                // judge whether j is in hj and gj,  [j, min{alpha+p-1,n}] and [alpha + p, min{j+p-1,n}],
                // [alpha + 1, j - 1]
                boolean is_contain_2p = false;
                for (int hg_j = alpha + 1; hg_j < j + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
                // judge whether j is in hj and gj,  [j, min{j+p-1,n}], [alpha + 1, min{alpha+p,n}]
                boolean is_contain_2p = false;
                for (int hg_j = alpha + 1; hg_j <= alpha + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                for (int hg_j = j; hg_j < j + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
                //        System.out.println("j_star_list adjust0n1");
            } else if (b.get(0) == raw_abs_sum) {
                j_star_list.add(block_size);
            }

        } // alpha == n
        else if (alpha < block_size && alpha >= block_size - p) {
            //      System.out.println("alpha == n");
            ArrayList<Integer> b;
            int j = 0;
            for (; j < alpha - p; j++) {
                // judge whether j is in hj and gj,  [j, j+p-1], [alpha + 1, min{alpha+p,n}]
                boolean is_contain_2p = false;
                for (int hg_j = j; hg_j < j + p; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                for (int hg_j = alpha + 1; hg_j <= alpha + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
                // judge whether j is in hj and gj,  [j, alpha-1] and [alpha + 1, min{j+p,n}], [j + p + 1,
                // min{alpha+p,n}]
                boolean is_contain_2p = false;
                for (int hg_j = j; hg_j <= alpha + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
                // judge whether j is in hj and gj,  [j, min{alpha+p-1,n}] and [alpha + p, min{j+p-1,n}],
                // [alpha + 1, j - 1]
                boolean is_contain_2p = false;
                for (int hg_j = alpha + 1; hg_j < j + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
                // judge whether j is in hj and gj,  [j, min{j+p-1,n}], [alpha + 1, min{alpha+p,n}]
                boolean is_contain_2p = false;
                for (int hg_j = alpha + 1; hg_j <= alpha + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                for (int hg_j = j; hg_j < j + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
                // judge whether j is in hj and gj,  [j, j+p-1], [alpha + 1, min{alpha+p,n}]
                boolean is_contain_2p = false;
                for (int hg_j = j; hg_j < j + p; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                for (int hg_j = alpha + 1; hg_j <= alpha + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
                // judge whether j is in hj and gj,  [j, alpha-1] and [alpha + 1, min{j+p,n}], [j + p + 1,
                // min{alpha+p,n}]
                boolean is_contain_2p = false;
                for (int hg_j = j; hg_j <= alpha + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
                // judge whether j is in hj and gj,  [j, min{alpha+p-1,n}] and [alpha + p, min{j+p-1,n}],
                // [alpha + 1, j - 1]
                boolean is_contain_2p = false;
                for (int hg_j = alpha + 1; hg_j < j + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
                // judge whether j is in hj and gj,  [j, min{j+p-1,n}], [alpha + 1, min{alpha+p,n}]
                boolean is_contain_2p = false;
                for (int hg_j = alpha + 1; hg_j <= alpha + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                for (int hg_j = j; hg_j < j + p && hg_j < block_size; hg_j++) {
                    if (max_index.contains(hg_j)) {
                        is_contain_2p = true;
                        break;
                    }
                }
                if (!is_contain_2p) {
                    continue;
                }
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
        //    System.out.println(j_star_list);
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
            length += (integers.get(0) - timestamp_delta_min);
            length += (integers.get(1) - value_delta_min);
        }
        ArrayList<Integer> b = new ArrayList<>();
        b.add(length);
//    int max_bit_width_interval = getBitWith(max_timestamp - timestamp_delta_min);
//    int max_bit_width_value = getBitWith(max_value - value_delta_min);
//    ArrayList<Integer> b = new ArrayList<>();
//    b.add(max_bit_width_interval);
//    b.add(max_bit_width_value);
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
            length += (integers.get(0) - timestamp_delta_min);
            length += (integers.get(1) - value_delta_min);
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
            length += (integers.get(0) - timestamp_delta_min);
            length += (integers.get(1) - value_delta_min);
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
            length += (integers.get(0) - timestamp_delta_min);
            length += (integers.get(1) - value_delta_min);
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
            length += (integers.get(0) - timestamp_delta_min);
            length += (integers.get(1) - value_delta_min);
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
                    if (epsilon_v_j > value_delta_max) {
                        value_delta_max = (int) epsilon_v_j;
                        value_delta_max_index = j;
                    }
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
                    if (epsilon_r_j > timestamp_delta_max) {
                        timestamp_delta_max = (int) epsilon_r_j;
                        timestamp_delta_max_index = j;
                    }
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
            ArrayList<ArrayList<Integer>> data, int block_size, int[] third_value,int segment_size ,int p) {
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
            length_time += encode2Bytes(ts_block_delta, raw_length, coefficient, result1, p).size();

            ArrayList<Integer> result2 = new ArrayList<>();
            splitTimeStamp3(ts_block_value, result2);
            ArrayList<Integer> raw_length_value = new ArrayList<>();
            ArrayList<Float> coefficient_value = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta_value = getEncodeBitsRegressionP(ts_block_value, block_size, raw_length_value, coefficient_value, p);
            length_value += encode2Bytes(ts_block_delta_value, raw_length_value, coefficient_value, result2, p).size();

            ArrayList<Integer> result3 = new ArrayList<>();
            splitTimeStamp3(ts_block_partition, result3);
            ArrayList<Integer> raw_length_partition = new ArrayList<>();
            ArrayList<Float> coefficient_partition = new ArrayList<>();
            ArrayList<ArrayList<Integer>> ts_block_delta_partition = getEncodeBitsRegressionP(ts_block_partition, block_size, raw_length_partition, coefficient_partition, p);
            length_partition += encode2Bytes(ts_block_delta_partition, raw_length_partition, coefficient_partition, result3, p).size();
        }
        if (length_partition < length_time && length_partition < length_value) { // partition performs better
            data = data_partition;
            System.out.println("type3");
//        for(int i=0;i<1;i++){
            for (int i = 0; i < block_num; i++) {
                ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();
                for (int j = 0; j < block_size; j++) {
                    ts_block.add(data.get(j + i * block_size));
                    ts_block_reorder.add(data.get(j + i * block_size));
                }

                ArrayList<Integer> result2 = new ArrayList<>();
                //      result2.add(1);
                splitTimeStamp3(ts_block, result2);

                ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
                ArrayList<Float> coefficient = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta =
                        getEncodeBitsRegressionP(ts_block, block_size, raw_length, coefficient ,p);


                // time-order
                quickSort(ts_block, 0, 0, block_size - 1);
                ArrayList<Integer> time_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
                ArrayList<Float> coefficient_time = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta_time =
                        getEncodeBitsRegressionP(ts_block, block_size, time_length, coefficient_time, p);

                // value-order
                quickSort(ts_block, 1, 0, block_size - 1);


                ArrayList<Integer> reorder_length = new ArrayList<>();
                ArrayList<Float> coefficient_reorder = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta_reorder =
                        getEncodeBitsRegressionP(ts_block, block_size, reorder_length, coefficient_reorder, p);

                int i_star;
                int j_star;
                int choose = min3(time_length.get(0),raw_length.get(0),reorder_length.get(0));
                if(choose == 0){
                    raw_length = time_length;
                    quickSort(ts_block, 0, 0, block_size - 1);
                    coefficient = coefficient_time;
                    ts_block_delta = ts_block_delta_time;
                    i_star = getIStarP(ts_block, block_size, 0, coefficient,p);
                } else if (choose == 1) {
                    ts_block = ts_block_reorder;
                    i_star = getIStarP(ts_block, block_size, 0, coefficient,p);
                }else {
                    raw_length = reorder_length;
                    coefficient = coefficient_reorder;
                    ts_block_delta = ts_block_delta_reorder;
                    i_star = getIStarP(ts_block, block_size, 1, coefficient,p);
                }
                j_star = getBetaP(ts_block, i_star, block_size, coefficient, p);

                int adjust_count = 0;
                while (j_star != -1 && i_star != -1) {
                    if (adjust_count < block_size / 2 && adjust_count <= 33) {
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

                    getEncodeBitsRegressionP(ts_block, block_size, raw_length, coefficient, p);
//        System.out.println("old_length"+old_length);
//        System.out.println("raw_length"+raw_length);
                    if (old_length.get(0) < raw_length.get(0)) {
                        ts_block = old_ts_block;
                        break;
                    }
                    //        System.out.println("adjust_count" + adjust_count);
//        System.out.println("i_star"+i_star);
                    i_star = getIStarP(ts_block, block_size, raw_length, coefficient, p);
                    if (i_star == j_star) break;
                    j_star = getBetaP(ts_block, i_star, block_size, coefficient, p);

                }

                ts_block_delta = getEncodeBitsRegressionP(ts_block, block_size, raw_length, coefficient, p);
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

//        ArrayList<Byte> cur_encoded_result = encode2Bytes(ts_block_delta, raw_length, theta, result2);
//        encoded_result.addAll(cur_encoded_result);

            }
        } else {
            if (length_value < length_time) { // order by value performs better
                System.out.println("type2");
                data = data_value;
            } else {
                System.out.println("type1");
            }
            for (int i = 0; i < block_num; i++) {
                ArrayList<ArrayList<Integer>> ts_block = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_reorder = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_partition = new ArrayList<>();
                for (int j = 0; j < block_size; j++) {
                    ts_block.add(data.get(j + i * block_size));
                    ts_block_reorder.add(data.get(j + i * block_size));
                }

                ArrayList<Integer> result2 = new ArrayList<>();
                //      result2.add(1);
                splitTimeStamp3(ts_block, result2);

                quickSort(ts_block, 0, 0, block_size - 1);
                ArrayList<Integer> raw_length = new ArrayList<>(); // length,max_bit_width_interval,max_bit_width_value,max_bit_width_deviation
                ArrayList<Float> coefficient = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta = getEncodeBitsRegressionP(ts_block, block_size, raw_length, coefficient, p);
                // value-order
                quickSort(ts_block, 1, 0, block_size - 1);

                ArrayList<Integer> reorder_length = new ArrayList<>();
                ArrayList<Float> coefficient_reorder = new ArrayList<>();
                ArrayList<ArrayList<Integer>> ts_block_delta_reorder = getEncodeBitsRegressionP( ts_block, block_size, reorder_length, coefficient_reorder, p);

                for (ArrayList<Integer> datum : ts_block) {
                    if (datum.get(1) > third_value[third_value.length - 1]) {
                        ts_block_partition.add(datum);
                    }
                }
                for(int third_i = third_value.length - 1;third_i>0;third_i--){
                    for (ArrayList<Integer> datum : ts_block) {
                        if (datum.get(1) <= third_value[third_i] && datum.get(1)>third_value[third_i-1]) {
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
                ArrayList<ArrayList<Integer>> ts_block_delta_partition = getEncodeBitsRegressionP( ts_block_partition, block_size, partition_length, coefficient_partition, p);
                int choose = min3(partition_length.get(0),reorder_length.get(0),raw_length.get(0));
                if(choose == 0){
                    raw_length = partition_length;
                    ts_block_delta = ts_block_delta_partition;
                    coefficient =  coefficient_partition;
                } else if (choose == 1) {
                    raw_length = reorder_length;
                    ts_block_delta = ts_block_delta_reorder;
                    coefficient =  coefficient_reorder;
                }


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


//    System.out.println("cur_bits:"+(encoded_result.size()*8L));
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
//            System.out.println(bit_width_time);
//            System.out.println(bit_width_value);
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

            // int max_bit_width_deviation = bytes2Integer(encoded, decode_pos, 4);
            // decode_pos += 4;
            // deviation_list = decodebitPacking(encoded,decode_pos,max_bit_width_deviation,0,block_size);
            // decode_pos += max_bit_width_deviation * (block_size - 1) / 8;

            int td_common = bytes2Integer(encoded, decode_pos, 4);
            decode_pos += 4;

            //      for (int i = 0; i < block_size-1; i++) {
            //        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
            //        ts_block_tmp.add(interval_list.get(i));
            //        ts_block_tmp.add(value_list.get(i));
            //        ts_block.add(ts_block_tmp);
            //      }

            //      ArrayList<Integer> tmp_data = new ArrayList<>();
            //      int timestamp = r0 * td + d0;
            //      tmp_data.add(timestamp);
            //      tmp_data.add(value0);
            //      data.add(tmp_data);

            int ti_pre = time0;
            int vi_pre = value0;
            for (int i = 0; i < block_size - 1; i++) {
                int ti = (int) ((double) theta1_r * ti_pre + (double) theta0_r + time_list.get(i));
                time_list.set(i, ti);
                ti_pre = ti;

                int vi = (int) ((double) theta1_v * vi_pre + (double) theta0_v + value_list.get(i));
                value_list.set(i, vi);
                vi_pre = vi;

                // ArrayList<Integer> ts_block_tmp = new ArrayList<>();
                // ts_block_tmp.add(time_list.get(i));
                // ts_block_tmp.add(value_list.get(i));
                // ts_block.add(ts_block_tmp);
            }

            ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
            ts_block_tmp0.add(time0);
            ts_block_tmp0.add(value0);
            ts_block.add(ts_block_tmp0);
            for (int i = 0; i < block_size - 1; i++) {
                // int ti = (time_list.get(i) - time0) * td_common  + time0 +
                // reverseZigzag(deviation_list.get(i));
                int ti = (time_list.get(i) - time0) * td_common + time0;
                ArrayList<Integer> ts_block_tmp = new ArrayList<>();
                ts_block_tmp.add(ti);
                ts_block_tmp.add(value_list.get(i));
                ts_block.add(ts_block_tmp);
            }

            quickSort(ts_block, 0, 0, block_size - 1);
            data.addAll(ts_block);

            //      int ri_pre = interval0;
            //      int vi_pre = value0;
            //      ArrayList<Integer> ts_block_tmp0 = new ArrayList<>();
            //      ts_block_tmp0.add(interval0);
            //      ts_block_tmp0.add(value0);
            //      ts_block.add(ts_block_tmp0);
            //      for (int i = 0; i < block_size-1; i++) {
            //        int ri = (int) (theta1_r * ri_pre + theta0_r + interval_list.get(i));
            //        interval_list.set(i,ri);
            //        ri_pre = ri;
            //
            //        int vi = (int) (theta1_v * vi_pre + theta0_v + value_list.get(i));
            //        value_list.set(i,vi);
            //        vi_pre = vi;
            //
            //        int dev; //zigzag
            //        if (deviation_list.get(block_size-1 - i - 1) % 2 == 0) {
            //          dev = deviation_list.get(block_size-1 - i - 1) / 2;
            //        } else {
            //          dev = -(deviation_list.get(block_size-1 - i - 1) + 1) / 2;
            //        }
            //        deviation_list.set(block_size-1 - i - 1,dev);
            //
            //        ArrayList<Integer> ts_block_tmp = new ArrayList<>();
            //        ts_block_tmp.add(interval_list.get(i));
            //        ts_block_tmp.add(value_list.get(i));
            //        ts_block.add(ts_block_tmp);
            //      }

            //      for(int i=0;i<block_size-1;i++){
            //        for(int j=0;j<block_size-1 -i -1;j++){
            //          if(interval_list.get(j)>interval_list.get(j+1)){
            //            int tmp_1 = interval_list.get(j);
            //            interval_list.set(j,interval_list.get(j+1));
            //            interval_list.set(j,tmp_1);
            //            int tmp_2 = value_list.get(j);
            //            value_list.set(j,value_list.get(j+1));
            //            value_list.set(j,tmp_2);
            //          }
            //        }
            //      }

            // quickSort(ts_block, 0, 0, block_size-2);
            // quickSort22(ts_block, 0, block_size-1);

            //      for (int i = 0; i < block_size; i++) {
            //        ArrayList<Integer> tmp_datai = new ArrayList<>();
            //        tmp_datai.add(ts_block.get(i).get(0));
            //        tmp_datai.add(ts_block.get(i).get(1));
            //        ts_block.set(i,tmp_datai);
            //      }

            //      for (int i = 0; i < block_size; i++) {
            //        ArrayList<Integer> tmp_datai = new ArrayList<>();
            //        tmp_datai.add(interval_list.get(i) * td + deviation_list.get(i-1) + r0 * td);
            //        tmp_datai.add(value_list.get(i));
            //        data.add(tmp_datai);
            //      }

            //      for (int i = 0; i < block_size-1; i++) {
            //        //int vi = vi_pre + value_list.get(i);
            //        int vi = vi_pre + ts_block.get(i).get(1);
            //        vi_pre = vi;
            //
            //        int ri = r0 * td + ts_block.get(i).get(0) * td;
            //
            //        int dev; //zigzag
            //        if (deviation_list.get(block_size-1 - i - 1) % 2 == 0) {
            //          dev = deviation_list.get(block_size-1 - i - 1) / 2;
            //        } else {
            //          dev = -(deviation_list.get(block_size-1 - i - 1) + 1) / 2;
            //        }
            //        int di = di_pre + dev;
            //        di_pre = di;
            //
            //        int timestampi = ri + (di + d0);
            //
            //        ArrayList<Integer> tmp_datai = new ArrayList<>();
            //        tmp_datai.add(timestampi);
            //        tmp_datai.add(vi);
            //        data.add(tmp_datai);
            //      }
        }
        return data;
    }


    public static void main(@org.jetbrains.annotations.NotNull String[] args) throws IOException {
        String parent_dir = "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\vldb\\compression_ratio\\p_float";
        String input_parent_dir = "C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\reorder\\iotdb_test_small\\";
        ArrayList<String> input_path_list = new ArrayList<>();
        ArrayList<String> output_path_list = new ArrayList<>();
        ArrayList<String> dataset_name = new ArrayList<>();
        ArrayList<Integer> dataset_block_size = new ArrayList<>();
        ArrayList<Integer> dataset_mid = new ArrayList<>();
        ArrayList<int[]> dataset_third = new ArrayList<>();
        //    input_path_list.add("C:\\Users\\xiaoj\\Documents\\GitHub\\encoding-reorder\\vldb\\test");
        //    output_path_list.add("C:\\Users\\xiaoj\\Desktop\\test.csv");
        //    dataset_block_size.add(1024);
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


        //
        // input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\Metro-Traffic");
        //
        // output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
        //            "\\p\\rr_int\\Metro-Traffic_ratio.csv");
        //    dataset_map_td.add(3600);
        //
        // input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\Nifty-Stocks");
        //
        // output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
        //            "\\p\\rr_int\\Nifty-Stocks_ratio.csv");
        //    dataset_map_td.add(86400);
        //
        // input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\USGS-Earthquakes");
        //
        // output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
        //            "\\p\\rr_int\\USGS-Earthquakes_ratio.csv");
        //    dataset_map_td.add(50);
        //
        // input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\Cyber-Vehicle");
        //
        // output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
        //            "\\p\\rr_int\\Cyber-Vehicle_ratio.csv");
        //    dataset_map_td.add(10);
        //    input_path_list.add(
        // "E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\TH-Climate");
        //
        // output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
        //            "\\p\\rr_int\\TH-Climate_ratio.csv");
        //    dataset_map_td.add(3);
        //
        // input_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\TY-Transport");
        //
        // output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
        //            "\\p\\rr_int\\TY-Transport_ratio.csv");
        //    dataset_map_td.add(5);
        //    input_path_list.add(
        // "E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\TY-Fuel");
        //
        // output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
        //            "\\p\\rr_int\\TY-Fuel_ratio.csv");
        //    dataset_map_td.add(60);
        //    input_path_list.add(
        // "E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\iotdb_test\\GW-Magnetic");
        //
        // output_path_list.add("E:\\thu\\Lab\\Group\\31编码论文\\encoding-reorder\\reorder\\result_evaluation" +
        //            "\\p\\rr_int\\GW-Magnetic_ratio.csv");
        //    dataset_map_td.add(100);

        //      for(int file_i=3;file_i<4;file_i++){
        for (int file_i = 0; file_i < input_path_list.size(); file_i++) {

            String inputPath = input_path_list.get(file_i);
            String Output = output_path_list.get(file_i);
            System.out.println(inputPath);
            //        String Output =  "C:\\Users\\xiaoj\\Desktop\\test_ratio.csv";

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
            //        for(int p=2;p<3;p++) {
            for (int p = 1; p < 10; p++) {
                System.out.println("p=" + p);
                for (File f : tempList) {
                    //        ArrayList<Integer> flag = new ArrayList<>();
                    //        flag.add(0);
                    //        flag.add(0);
                    //        flag.add(0);

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
                        for (int repeat_i = 0; repeat_i < 10; repeat_i++)
                            buffer =
                                    ReorderingRegressionEncoder(data, dataset_block_size.get(file_i), dataset_third.get(file_i), 8, p);

                        long e = System.nanoTime();
                        encodeTime += ((e - s) / 10);
                        compressed_size += buffer.size();
                        double ratioTmp = (double) buffer.size() / (double) (data.size() * Integer.BYTES * 2);
                        ratio += ratioTmp;
                        s = System.nanoTime();
                        //          data_decoded =
                        // ReorderingRegressionDecoder(buffer,dataset_map_td.get(file_i));
                        e = System.nanoTime();
                        decodeTime += ((e - s) / 10);

                        //          for(int j=0;j<256;j++){
                        //            if(!data.get(j).get(0).equals(data_decoded.get(j).get(0))){
                        //              System.out.println("Wrong Time!");
                        //              System.out.print(j);
                        //              System.out.print(" ");
                        //              System.out.print(data.get(j).get(0));
                        //              System.out.print(" ");
                        //              System.out.println(data_decoded.get(j).get(0));
                        //            }
                        //            else{
                        //              System.out.println("Correct Time!");
                        //              System.out.print(j);
                        //              System.out.print(" ");
                        //              System.out.print(data.get(j).get(0));
                        //              System.out.print(" ");
                        //              System.out.println(data_decoded.get(j).get(0));
                        //            }
                        //            if(!data.get(j).get(1).equals(data_decoded.get(j).get(1))){
                        //              System.out.println("Wrong Value!");
                        //              System.out.print(j);
                        //              System.out.print(" ");
                        //              System.out.print(data.get(j).get(1));
                        //              System.out.print(" ");
                        //              System.out.println(data_decoded.get(j).get(1));
                        //            }
                        //            else{
                        //              System.out.println("Correct Value!");
                        //              System.out.print(j);
                        //              System.out.print(" ");
                        //              System.out.print(data.get(j).get(1));
                        //              System.out.print(" ");
                        //              System.out.println(data_decoded.get(j).get(1));
                        //            }
                        //          }
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
                              System.out.println(ratio);
                    writer.writeRecord(record);
                    //          break;
                }
            }
            writer.close();
        }
    }

}
