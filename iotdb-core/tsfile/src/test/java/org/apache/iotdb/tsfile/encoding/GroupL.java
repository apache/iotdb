package org.apache.iotdb.tsfile.encoding;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import static java.lang.Math.pow;

public class GroupL {
    public int[] number;
    public int count;
    public int[] count_array;
    public int range;
    public long[] sorted_value_list;
    public int unique_number;
    public int mask;
    public int left_shift;

    HashMap<Integer, int[]> map = new HashMap<>();


    public void getBetaArray(int max_delta_value, int alpha, GroupU[] groupU){
        int max_bit_width = getBitWith(max_delta_value) + 1;
        int min_this_group = (int) pow(2,alpha-1); // down line
        int max_this_group = (int) pow(2,alpha)-1; // up line
        int gamma_max = getBitWith(max_delta_value-min_this_group);
        for(int i=1; i<=gamma_max; i++){ // transverse every gamma
            int gamma_pow_i = (int) pow(2,i);
            int k2_end = max_delta_value-gamma_pow_i; // left line
            int[] cur_cur_k2_array = new int[max_bit_width*2];
            int cur_cur_k2_index = 0;
            GroupU cur_group_gamma = groupU[i];
            int gamma_unique_number = cur_group_gamma.unique_number;
            long[] gamma_sorted = cur_group_gamma.sorted_value_list;
            int gamma_pow_i_1 = (int) pow(2,i-1);
            int k2_start = (int) (max_delta_value-gamma_pow_i_1);
            int min_beta = getBitWith(k2_end - max_this_group);
            int max_beta = getBitWith(k2_start - min_this_group);
            for(int beta = min_beta;beta<=max_beta;beta++){
//                if(beta>=alpha && beta >= i) break;

                int pow_2_beta = (int) pow(2,beta);
                int x_u_i_end = 0;
//                if(alpha > beta ) {
//                    System.out.println("ads0d0s0d0s0");
                    x_u_i_end = k2_start - (pow_2_beta + min_this_group);
//                }else {
//                    x_u_i_end = k2_start - (pow_2_beta + max_this_group);
//                }
//                int accumulate = gamma_unique_number>0? cur_group_gamma.getCount(gamma_sorted[0]):0;
                if(x_u_i_end<gamma_pow_i_1 && x_u_i_end>0)
                    for (int unique_i = 0; unique_i < gamma_unique_number; unique_i++) {
                        int x_u_i = cur_group_gamma.getUniqueValue(gamma_sorted[unique_i]);
    //                    int cur_cur_k2 = cur_group_gamma.getCount(gamma_sorted[unique_i+1]);

    //                    accumulate += cur_cur_k2;
                        if (x_u_i > x_u_i_end) {
                            int cur_cur_k2 = cur_group_gamma.getCount(gamma_sorted[unique_i]);
                            cur_cur_k2_array[cur_cur_k2_index] = x_u_i;
                            cur_cur_k2_index ++;
                            cur_cur_k2_array[cur_cur_k2_index] = cur_cur_k2;
                            cur_cur_k2_index ++;
                            break;
                        }
                    }

            }
            int[] cur_cur_k2_array_new = new int[cur_cur_k2_index];
            System.arraycopy(cur_cur_k2_array, 0, cur_cur_k2_array_new, 0, cur_cur_k2_index);
            map.put(i, cur_cur_k2_array_new);
        }

    }
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
    public GroupL(int[] number, int count, int i) {
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

    public void setCount_array(){
        double par = this.range/(count*Math.log(this.count));
        int k1_start = this.range;
        this.left_shift = getBitWith(this.count);
        this.mask = (1 << left_shift) - 1;
        if (par > 3) {
            int[] value_list = new int[this.count];
            for (int i = 0; i < this.count; i++) {
                int value = this.number[i];
                count_array[value - k1_start]++;
                if (count_array[value - k1_start] == 1) {
                    value_list[unique_number] = value;
                    unique_number++;
                }
            }
            sorted_value_list = new long[unique_number];
            for (int i = 0; i < unique_number; i++) {
                int value = value_list[i];
                sorted_value_list[i] = (((long) (value - k1_start)) << left_shift) + count_array[value - k1_start];
            }
            Arrays.sort(sorted_value_list);

            int cdf_count = 0;
            for (int i = 0; i < unique_number; i++) {
                cdf_count += getCount(sorted_value_list[i]);
                sorted_value_list[i] = (((long) getUniqueValue(sorted_value_list[i])) << left_shift) + cdf_count;//new_value_list[i]
            }
        }else{
            int[] hash = new int[this.range];
            for (int i = 0; i < this.count; i++) {
                int value = this.number[i];
                if (hash[value - k1_start] == 0){
                    this.unique_number++;
                }
                hash[value - k1_start]++;
            }
            sorted_value_list = new long[unique_number];
            int ccount = 0;
            int index = 0;
            for(int i=0;i<this.range;i++){
                if (hash[i] >0) {
                    ccount += hash[i];
                    sorted_value_list[index] = (((long) i) << left_shift) + ccount;
                    index++;
                }
            }
        }
    }

    public void incrementCount() {
        count++;
    }

    @Override
    public String toString() {
        return "Number: " + number + ", Count: " + count;
    }

}
