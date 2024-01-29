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

        this.unique_count_array = new int[range];
        this.unique_number_array = new int[range];
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
        int unique_value_count = 0;
        //int[] value_count_list = new int[this.range];
        int[] number_gamma = this.number;
        for (int i = 0; i < this.count; i++) {
            int value = number_gamma[i];
            count_array[k2_end - value]++;
            value_list[unique_value_count] = value;
            unique_value_count ++;
        }
        // O(count log(count))    count<block_size O(count log(count)) < O(10*1024)  O(range)
//        for(int value:this.number){
//            if(value_count_list[value]==0){
//                value_count_list[value] = 1;
//                value_list[unique_value_count] = value;
//                unique_value_count ++;
//            }else{
//                value_count_list[value] ++;
//            }
//        }
        
        long[] sorted_value_list = new long[unique_value_count];
        int count = 0;
//        int[] new_value_list = new int[unique_value_count];
        for(int i=0;i<unique_value_count;i++){
            int value = value_list[i];
//            new_value_list[i] = value;
//            count += value_count_list[value];
            sorted_value_list[i] = (((long) value) << left_shift) + count_array[k2_end - value];
        }
        Arrays.sort(sorted_value_list);
//        Arrays.sort(new_value_list);
        for(int i=0;i<unique_value_count;i++){
            unique_count_array[i] = getCount(sorted_value_list[i]);
            unique_number_array[i]= getUniqueValue(sorted_value_list[i]);
        }
//        for(int i=0;i<unique_value_count;i++){
//            count += getCount(sorted_value_list[i], mask);
//            sorted_value_list[i] = (((long)getUniqueValue(sorted_value_list[i], left_shift) ) << left_shift) + count;//new_value_list[i]
//        }
//        for(int i=0;i<range;i++){
//            if(count_array[i]!=0){
//                unique_count_array[unique_number] = count_array[i];
//                unique_number_array[unique_number]= i;
//                unique_number++;
//            }
//        }
    }

    public void incrementCount() {
        count++;
    }

    @Override
    public String toString() {
        return "Number: " + number + ", Count: " + count;
    }

}
