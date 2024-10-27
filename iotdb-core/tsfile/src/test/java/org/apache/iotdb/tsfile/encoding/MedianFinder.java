package org.apache.iotdb.tsfile.encoding;

import java.util.Arrays;

import static org.apache.iotdb.tsfile.constant.TestConstant.random;

public class MedianFinder {

    int approximate_parameter = 8;
        public static int findMedian(int[] arr) {
            if (arr == null || arr.length == 0) {
                throw new IllegalArgumentException("数组不能为空");
            }
            int n = arr.length;
            return quickSelect(arr, 0, n - 1, n / 2);
        }

    private static int medianOfMedians(int[] arr, int left, int right) {
        int numElements = right - left + 1;
        if (numElements <= 3) {
            Arrays.sort(arr, left, right + 1);
            return arr[left + (numElements / 2)];
        }

        for (int i = 0; i < (numElements + 2) / 3 - 1; i++) {
            int subLeft = left + i * 3;
            int subRight = subLeft + 2;
            if (subRight > right) subRight = right;

            Arrays.sort(arr, subLeft, subRight + 1);
            int medianIndex = subLeft + 1;  // Median of 5 elements
            swap(arr, left + i, medianIndex);
        }

        return medianOfMedians(arr, left, left + (numElements + 2) / 3 - 1);
    }
//    private static int medianOfMedians(int[] arr, int left, int right) {
//        if (right - left + 1 <= 8) {
//            Arrays.sort(arr, left, right + 1);
//            return arr[left + (right - left) / 2];  // 在小组内找中位数
//        }
//
//        int numMedians = 0;
//        for (int i = left; i <= right - 8; i += 8) {
//            int subRight = Math.min(i + 7, right);  // 每组8个元素
//            int median = medianOfMedians(arr, i, subRight);  // 递归找中位数
//            swap(arr, left + numMedians, i + 3);  // 将中位数换到数组前面的位置
//            numMedians++;
//        }
//        return medianOfMedians(arr, left, left + numMedians - 1);  // 对中位数递归调用
//    }

    public static int quickSelect(int[] arr, int left, int right, int k) {
        if (left == right) {
            return arr[left];
        }
        if (areAllElementsEqual(arr, left, right)) {
            return arr[left];
        }
        int pivotIndex = partition(arr, left, right, medianOfMedians(arr, left, right));

        if (k == pivotIndex) {
            return arr[k];
        } else if (k < pivotIndex) {
            return quickSelect(arr, left, pivotIndex - 1, k);
        } else {
            return quickSelect(arr, pivotIndex + 1, right, k);
        }
    }
    private static boolean areAllElementsEqual(int[] arr, int left, int right) {
        for (int i = left + 1; i <= right; i++) {
            if (arr[i] != arr[left]) {
                return false;
            }
        }
        return true;
    }
    private static int partition(int[] arr, int left, int right, int pivot) {
        while (left <= right) {
            while (arr[left] < pivot) {
                left++;
            }
            while (arr[right] > pivot) {
                right--;
            }
            if (left <= right) {
                swap(arr, left, right);
                left++;
                right--;
            }
        }
        return left - 1;
    }

    private static void swap(int[] arr, int i, int j) {
        int temp = arr[i];
        arr[i] = arr[j];
        arr[j] = temp;
    }

    public static void main(String[] args) {
        int[] array = { 12, 3, 5, 7, 4, 19, 26, 23, 31, 2, 29, 18, 17, 15, 8, 9, 10, 24, 25, 27, 28, 30, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43 };
        int n = array.length;
        int k = n / 2;
        System.out.println("Median is " + quickSelect(array, 0, n - 1, k));
    }

//    public static int findMedian(int[] arr) {
//        if (arr == null || arr.length == 0) {
//            throw new IllegalArgumentException("数组不能为空");
//        }
//        int n = arr.length;
//        return medianOfMedians(arr, 0, n - 1, n / 2);
//    }
//
//    private static int medianOfMedians(int[] arr, int left, int right, int k) {
//        if (left == right) {
//            return arr[left];
//        }
//
//        if (areAllElementsEqual(arr, left, right)) {
//            return arr[left];
//        }
//        // 随机选择一个pivot索引
//        int pivotIndex = left + random.nextInt(right - left + 1);
//        // 把随机选的pivot放到最后
//        swap(arr, pivotIndex, right);
//        // 计算中位数的中位数的近似值
////        int pivotIndex = getPivotIndex(arr, left, right);
////
////        // 使用该中位数进行划分
//        pivotIndex = partition(arr, left, right, pivotIndex);
//
//        // 递归地查找目标值
//        if (k == pivotIndex) {
//            return arr[k];
//        } else if (k < pivotIndex) {
//            return medianOfMedians(arr, left, pivotIndex - 1, k);
//        } else {
//            return medianOfMedians(arr, pivotIndex + 1, right, k);
//        }
//    }
//    private static boolean areAllElementsEqual(int[] arr, int left, int right) {
//        for (int i = left + 1; i <= right; i++) {
//            if (arr[i] != arr[left]) {
//                return false;
//            }
//        }
//        return true;
//    }
//
//// 获取中位数的中位数的近似值
//    private static int getPivotIndex(int[] arr, int left, int right) {
//        int n = right - left + 1;
//        int numMedians = (n + 4) / 5; // 每组5个元素，共有numMedians组
//
//        int[] medians = new int[numMedians];
//        int[] medianIndices = new int[numMedians];
//        for (int i = 0; i < numMedians; i++) {
//            int subLeft = left + i * 5;
//            int subRight = Math.min(subLeft + 4, right);
//            medians[i] = getMedian(arr, subLeft, subRight);
//            medianIndices[i] = getMedianIndex(arr, subLeft, subRight, medians[i]);
//        }
//
//        // 使用递归方法找到这些中位数的中位数
//        int medianOfMediansValue = medianOfMedians(medians, 0, medians.length - 1, medians.length / 2);
//
//        // 返回该中位数的原数组索引
//        for (int i = 0; i < numMedians; i++) {
//            if (medians[i] == medianOfMediansValue) {
//                return medianIndices[i];
//            }
//        }
//        return -1; // This shouldn't happen if logic is correct
//    }
//
//    // 获取给定范围内数组的中位数
//    private static int getMedian(int[] arr, int left, int right) {
//        int[] subArray = Arrays.copyOfRange(arr, left, right + 1);
//        Arrays.sort(subArray);
//        return subArray[subArray.length / 2];
//    }
//
//    // 获取给定范围内某个数值在数组中的索引
//    private static int getMedianIndex(int[] arr, int left, int right, int medianValue) {
//        for (int i = left; i <= right; i++) {
//            if (arr[i] == medianValue) {
//                return i;
//            }
//        }
//        return -1; // This shouldn't happen if logic is correct
//    }
//
//
//    private static int partition(int[] arr, int left, int right, int pivotIndex) {
//        int pivot = arr[pivotIndex];
//        swap(arr, pivotIndex, right);
//        int i = left;
//
//        for (int j = left; j < right; j++) {
//            if (arr[j] <= pivot) {
//                swap(arr, i, j);
//                i++;
//            }
//        }
//        swap(arr, i, right);
//        return i;
//    }
//
//    private static void swap(int[] arr, int i, int j) {
//        int temp = arr[i];
//        arr[i] = arr[j];
//        arr[j] = temp;
//    }
//
//    public static void main(String[] args) {
//        int[] arr = {7, 10, 4, 3, 20, 15};
//        System.out.println("中位数为: " + findMedian(arr));
//    }
}

