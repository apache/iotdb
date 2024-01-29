package org.apache.iotdb.tsfile.encoding;

import java.util.Arrays;

import static java.lang.Math.pow;

public class GroupU{

    public int[] number;
    public int count;
    public int if_count;
    public int[] count_array;
    public int range;

    public int[] unique_number_array;
    public int[] unique_count_array;
    public int unique_number;
    public int mask;
    public int left_shift;

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
        this.if_count = 0;
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

    public void setCount_array(int k2_end){
        this.left_shift = getBitWith(this.count);
        this.mask =  (1 << left_shift) - 1; //block_size*2-1; //
        this.if_count = 1;
        int[] value_list = new int[this.count];
        int[] number_gamma = this.number;
        for (int i = 0; i < this.count; i++) {
            int value = number_gamma[i];
            count_array[k2_end - value]++;
            if (count_array[k2_end - value] == 1) {
                value_list[unique_number] = value;
                unique_number++;
            }
        }
        this.unique_count_array = new int[unique_number];
        this.unique_number_array = new int[unique_number];
        long[] sorted_value_list = new long[unique_number];
        for(int i=0;i<unique_number;i++){
            int value = value_list[i];
            sorted_value_list[i] = (((long) (k2_end - value)) << left_shift) + count_array[k2_end - value];
        }
        Arrays.sort(sorted_value_list);
        for(int i=0;i<unique_number;i++){
            unique_count_array[i] = getCount(sorted_value_list[i]);
            unique_number_array[i]= getUniqueValue(sorted_value_list[i]);
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
