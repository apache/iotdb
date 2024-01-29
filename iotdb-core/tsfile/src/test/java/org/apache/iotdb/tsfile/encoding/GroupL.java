package org.apache.iotdb.tsfile.encoding;

import static java.lang.Math.pow;

public class GroupL {
    public int[] number;
    public int count;
    public int if_count;
    public int[] count_array;
    public int range;
    public int[] unique_number_array;
    public int[] unique_count_array;
    public int unique_number;

    public GroupL(int[] number, int count, int i) {
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

    public void setCount_array(int k1_start){
        this.if_count = 1;
        int[] number_gamma = this.number;
        for (int i = 0; i < this.count; i++) {
            int value = number_gamma[i];
            count_array[value - k1_start]++;
        }
        for(int i=0;i<range;i++){
            if(count_array[i]!=0){
                unique_count_array[unique_number] = count_array[i];
                unique_number_array[unique_number]= i;
                unique_number++;
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
