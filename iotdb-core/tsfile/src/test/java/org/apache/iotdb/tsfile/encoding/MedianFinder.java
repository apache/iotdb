package org.apache.iotdb.tsfile.encoding;

import java.util.Arrays;

public class MedianFinder {

    public static int findMedian(int[] arr) {
        if (arr == null || arr.length == 0) {
            throw new IllegalArgumentException("数组不能为空");
        }
        int n = arr.length;
        return medianOfMedians(arr, 0, n - 1, n / 2);
    }

    private static int medianOfMedians(int[] arr, int left, int right, int k) {
        if (left == right) {
            return arr[left];
        }

        // 计算中位数的中位数的近似值
        int pivotIndex = getPivotIndex(arr, left, right);

        // 使用该中位数进行划分
        pivotIndex = partition(arr, left, right, pivotIndex);

        // 递归地查找目标值
        if (k == pivotIndex) {
            return arr[k];
        } else if (k < pivotIndex) {
            return medianOfMedians(arr, left, pivotIndex - 1, k);
        } else {
            return medianOfMedians(arr, pivotIndex + 1, right, k);
        }
    }

    // 获取中位数的中位数的近似值
    private static int getPivotIndex(int[] arr, int left, int right) {
        int n = right - left + 1;
        int numMedians = (n + 4) / 5; // 每组5个元素，共有numMedians组

        int[] medians = new int[numMedians];
        for (int i = 0; i < numMedians; i++) {
            int subLeft = left + i * 5;
            int subRight = Math.min(subLeft + 4, right);
            medians[i] = getMedian(arr, subLeft, subRight);
        }

        // 使用递归方法找到这些中位数的中位数
        return medianOfMedians(medians, 0, medians.length - 1, medians.length / 2);
    }

    // 获取给定范围内数组的中位数
    private static int getMedian(int[] arr, int left, int right) {
        int[] subArray = Arrays.copyOfRange(arr, left, right + 1);
        Arrays.sort(subArray);
        return subArray[subArray.length / 2];
    }

    private static int partition(int[] arr, int left, int right, int pivotIndex) {
        int pivot = arr[pivotIndex];
        swap(arr, pivotIndex, right);
        int i = left;

        for (int j = left; j < right; j++) {
            if (arr[j] <= pivot) {
                swap(arr, i, j);
                i++;
            }
        }
        swap(arr, i, right);
        return i;
    }

    private static void swap(int[] arr, int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    public static void main(String[] args) {
        int[] arr = {7, 10, 4, 3, 20, 15};
        System.out.println("中位数为: " + findMedian(arr));
    }
}
