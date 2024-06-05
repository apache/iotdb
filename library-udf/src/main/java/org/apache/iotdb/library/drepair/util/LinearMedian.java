package org.apache.iotdb.library.drepair.util;

public class LinearMedian {
    //Function to invoke LinearSelect
    public static double getMedian(double[] S, int size) {
        return linearSelect(0, size - 1, S, size / 2);
    }

    //do linearSelect in a recursive way
    private static double linearSelect(int left, int right, double[] array, int k) {
        //if there is only one element now, just record.
        if (left >= right) {
            return array[left];
        }
        //do the partition
        int p = pickCleverPivot(left, right, array);
        int eIndex = partition(left, right, array, p);
        //after the partition, do following ops
        if (k <= eIndex) {
            return linearSelect(left, eIndex - 1, array, k);
        } else if (k == eIndex + 1) {
            return array[eIndex];
        } else {
            return linearSelect(eIndex + 1, right, array, k);
        }

    }

    //do Partition with a pivot
    private static int partition(int left, int right, double[] array, int pIndex) {
        //move pivot to last index of the array
        swap(array, pIndex, right);

        double p = array[right];
        int l = left;
        int r = right - 1;

        while (l <= r) {
            while (l <= r && array[l] <= p) {
                l++;
            }
            while (l <= r && array[r] >= p) {
                r--;
            }
            if (l < r) {
                swap(array, l, r);
            }
        }

        swap(array, l, right);
        return l;
    }

    //Pick a random pivot to do the LinearSelect
    //Implementation inspired from pseudocode provided in assignment
    //instructions and at https://en.wikipedia.org/wiki/Median_of_medians
    private static int pickCleverPivot(int left, int right, double[] array) {
        int n = array.length;

        //Base case: If array length is less than 5, get median of it
        if ((right - left) < 5) {
            return getMedianValue(left, right, array);
        }//end if

        int count = left;

        //Divide array into subgroups of 5
        //Sort respective subarray to retrieve its median
        for (int i = left; i <= right; i += 5) {

            int tempRight = i + 4;

            if (tempRight > right) {
                tempRight = right;
            }//end if

            int medianSubgroup;

            if ((tempRight - i) <= 2) {
                continue;
            } else {
                medianSubgroup = getMedianValue(i, tempRight, array); //Retrieve median of subarray
            }

            //Swap median to front of array
            swap(array, medianSubgroup, count);

            count++;
        }//end for

        //Recursively call pickCleverPivot for median of medians
        return pickCleverPivot(left, count, array);
    }//end pickCleverPivot


    private static int getMedianValue(int left, int right, double[] array) {

        //Sort that subarray! (Using insertion sort that is.)
        for (int i = left; i <= right; i++) {
            int j = i;
            while (j > left && array[j - 1] > array[j]) {
                swap(array, j, j - 1);
                j -= 1;
            }//end while
        }//end for


        int medianPos = 0;

        //Check if length of subarray is even or not
        //Implementation inspired by code found here: http://stackoverflow.com/a/11955900
        if ((right - left) % 2 == 0) {
            //Retrieve value left of middle to be treated as median
            medianPos = ((right - left) / 2) - 1;
        } else {
            medianPos = (right - left) / 2;
        }//end if/else statement

        return left + medianPos;

    }//end getMedianValue


    //swap two elements in the array
    private static void swap(double[] array, int a, int b) {
        double tmp = array[a];
        array[a] = array[b];
        array[b] = tmp;
    }
}
