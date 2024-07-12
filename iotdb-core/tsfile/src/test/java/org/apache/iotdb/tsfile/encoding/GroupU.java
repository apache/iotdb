package org.apache.iotdb.tsfile.encoding;

import java.util.Arrays;
import java.util.HashMap;

import static java.lang.Math.pow;

public class GroupU{

    public int[] number;
    public int count;
    public int[] count_array;
    public int range;
    public int unique_number;
    public int mask;
    public int left_shift;
    public long[] sorted_value_list; // cdf
    public int[] invert;
    public int[] invert2;
    HashMap<Integer, int[]> map = new HashMap<>();


    public void getBetaArray(int max_delta_value, int gamma, GroupL[] groupL) {
        int max_bit_width = getBitWith(max_delta_value) + 1;
        int max_this_group = max_delta_value - (int) pow(2, gamma - 1);
        int min_this_group = max_delta_value - (int) pow(2, gamma) + 1;
        int alpha_max = getBitWith(max_delta_value-min_this_group);
        for (int i = 1; i <= alpha_max; i++) {
            int k1_start = (int) pow(2, i-1);
//            int k2_end = (int) (max_delta_value - pow(2, i));
            if (k1_start > max_this_group) break;
            int[] cur_cur_k1_array = new int[max_bit_width * 2];
            int cur_cur_k1_index = 0;
            GroupL cur_group_alpha = groupL[i - 1];
            int alpha_unique_number = cur_group_alpha.unique_number;
            long[] alpha_sorted = cur_group_alpha.sorted_value_list;
            int k1_end = (int) ( pow(2, i));
            int max_beta = getBitWith(max_this_group - k1_start);
            int min_beta = getBitWith(min_this_group- k1_end);
            for (int beta = min_beta; beta <= max_beta; beta++) {
//                if(beta>=gamma && beta >= i) break;
                int pow_2_beta = (int) pow(2, beta);
                int x_l_i_end = 0;
//                if(gamma > beta ){
                    x_l_i_end =  max_this_group - ( pow_2_beta + k1_start);
//                }else {
//                    x_l_i_end = max_this_group - (pow_2_beta + k1_end + 2);
//                }

//                int accumulate = alpha_unique_number>0 ? cur_group_alpha.getCount(alpha_sorted[0]):0;
//                int x_u_i_end = k2_end - (pow_2_beta + min_this_group + 2);
                if(x_l_i_end<k1_start && x_l_i_end>0)
                    for (int unique_i = 0; unique_i < alpha_unique_number - 1; unique_i++) {
                        int x_l_i = cur_group_alpha.getUniqueValue(alpha_sorted[unique_i]);
//                        int cur_cur_k1 = cur_group_alpha.getCount(alpha_sorted[unique_i+1]);
//                        accumulate += cur_cur_k1;
                        if (x_l_i > x_l_i_end) {
                            int cur_cur_k1 = cur_group_alpha.getCount(alpha_sorted[unique_i]);
                            cur_cur_k1_array[cur_cur_k1_index] = x_l_i;
                            cur_cur_k1_index++;
                            cur_cur_k1_array[cur_cur_k1_index] = cur_cur_k1;
                            cur_cur_k1_index++;
                            break;
                        }
                    }

            }
            int[] cur_cur_k1_array_new = new int[cur_cur_k1_index];
            System.arraycopy(cur_cur_k1_array, 0, cur_cur_k1_array_new, 0, cur_cur_k1_index);
            map.put(i, cur_cur_k1_array_new);
        }
    }
//        public static int getBitWithgamma(int num) {
//            if (num == 0) return 1;
//            else return 32 - Integer.numberOfLeadingZeros(num);
//        }
    public int getCount(long long1) {
        return ((int) (long1 & this.mask));
    }
    public int getUniqueValue(long long1) {
        return ((int) ((long1) >> this.left_shift));
    }

    public static int getBitWith(int num) {
        if (num == 0) return 1;
        else return 32 - Integer.numberOfLeadingZeros(num);
    }

    public GroupU(int[] number, int count, int i) {
        this.number = number;
        this.count = count;
        this.range = (int) pow(2,i-1);
        this.count_array = new int[range];
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

    public void setCount_array(int max_){
        int k2_end = max_ - this.range;
        double par = this.range/(count*Math.log(this.count));
        this.left_shift = getBitWith(this.count);
        this.mask = (1 << left_shift) - 1;
        invert = new int[this.range+1];
        if (par > 3) {
            int[] value_list = new int[this.count];
            for (int i = 0; i < this.count; i++) {
                int value = this.number[i];
                count_array[k2_end - value]++;
                if (count_array[k2_end - value] == 1) {
                    value_list[unique_number] = value;
                    unique_number++;
                }
            }
            sorted_value_list = new long[unique_number];
            for (int i = 0; i < unique_number; i++) {
                int value = value_list[i];
                sorted_value_list[i] = (((long) (k2_end - value)) << left_shift) + count_array[k2_end - value];
            }
            Arrays.sort(sorted_value_list);

            int cdf_count = 0;
            for (int i = 0; i < unique_number; i++) {
                cdf_count += getCount(sorted_value_list[i]);
                sorted_value_list[i] = (((long) getUniqueValue(sorted_value_list[i])) << left_shift) + cdf_count;//new_value_list[i]
            }
        }else {
            int[] hash = new int[this.range];
            for (int i = 0; i < this.count; i++) {
                int value = this.number[i];
                if (hash[k2_end - value] == 0) {
                    this.unique_number++;
                }
                hash[k2_end - value]++;
            }
            sorted_value_list = new long[unique_number];
            int ccount = 0;
            int index = 0;
            for (int i = 0; i < this.range; i++) {
                if (hash[i] > 0) {
                    ccount += hash[i];
                    sorted_value_list[index] = (((long) i) << left_shift) + ccount;
                    index++;
                }
            }
        }
    }

    public void setinvert(){
        int count = 0;
        for (int i = 0; i < this.range; i++) {
            invert[i] = count;
            if (count < unique_number && getUniqueValue(sorted_value_list[count]) == i){
                count ++;
            }
        }
        invert[this.range] = count;
    }

    public void incrementCount() {
        count++;
    }

    @Override
    public String toString() {
        return "Number: " + number + ", Count: " + count;
    }

}
