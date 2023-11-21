package org.apache.iotdb.tsfile.encoding;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KernelDensityEstimation {
        public static void main(String[] args) {
                // 第一个整数（正数）
                int int1 = -1;
                // 第二个整数（负数）
                int int2 = -67890;

                // 将两个整数拼接成一个长整型数
                long combined = ((long) int1 << 32) | (int2 & 0xFFFFFFFFL);

                System.out.println("Combined long value: " + combined);

                // 将长整型数分解回两个整数
                int originalInt1 = (int) (combined >> 32);
                int originalInt2 = (int) combined;

                System.out.println("Original Int 1: " + originalInt1);
                System.out.println("Original Int 2: " + originalInt2);

            // 离散分布数据
//            Map<Integer, Integer> discreteDistribution = new HashMap<>();
//            discreteDistribution.put(2, 10);
//            discreteDistribution.put(3, 100);
//            discreteDistribution.put(1, 3);
//            discreteDistribution.put(4, 12);
//
//            // 计算核密度估计
//            double[] kernelDensity = calculateKernelDensity(discreteDistribution);
//
//            // 打印核密度估计
//            System.out.println("Kernel Density Estimation:");
//            for (int i = 0; i < kernelDensity.length; i++) {
//                System.out.println("x=" + (i + 1) + ": " + kernelDensity[i]);
//            }
//
//            // 寻找核密度估计的极小值点
//            int[] minIndex = findMinIndex(kernelDensity);
//            System.out.println("Minimum point: x=" + (Arrays.toString(minIndex)));
        }

        // 计算核密度估计
        static double[] calculateKernelDensity(Map<Integer, Integer> discreteDistribution) {
            int maxKey = discreteDistribution.keySet().stream().max(Integer::compare).orElse(0);
            double[] kernelDensity = new double[maxKey];

            for (int x = 1; x <= maxKey; x++) {
                for (Map.Entry<Integer, Integer> entry : discreteDistribution.entrySet()) {
                    int dataPoint = entry.getKey();
                    int weight = entry.getValue();
                    kernelDensity[x - 1] += gaussianKernel(x, dataPoint) * weight;
                }
            }

            return kernelDensity;
        }

        // 高斯核函数
        private static double gaussianKernel(int x, int dataPoint) {
            double bandwidth = 1.0; // 可调整的带宽参数
            return Math.exp(-0.5 * Math.pow((x - dataPoint) / bandwidth, 2)) / (Math.sqrt(2 * Math.PI) * bandwidth);
        }

        // 寻找数组中的最小值索引
        static int[] findMinIndex(double[] array) {
            int[] minIndex = new int[array.length];
            int final_min_count = 0;
            int pre_value = 0;
//            double preValue = array[0];

            for (int i = 1; i < array.length-1; i++) {
                if (array[i] < array[i-1] && array[i] < array[i+1]) {
                    if(final_min_count != 0){
                        if(i>pre_value+32){
                            minIndex[final_min_count] = i;
                            final_min_count ++;
                            pre_value = i;
                        }
                    }else{
                        minIndex[final_min_count] = i;
                        final_min_count ++;
                        pre_value = i;
                    }
                }
            }
            int[] final_minIndex = new int[final_min_count];
//            if(final_min_count>0){
//                final_minIndex[0] = minIndex[0];
//                int pre_value = minIndex[0];
//                for(int mv = 1; mv<final_min_count;mv++){
//                    if(minIndex[mv]-pre_value>16){
//                        pre_value = minIndex[mv];
//
//                    }
//                }
            System.arraycopy(minIndex, 0, final_minIndex, 0, final_min_count);
//            }
            return final_minIndex;
        }
//    public static void main(String[] args) {
//        // 数据分布
//        Map<Integer, Integer> data = new HashMap<>();
//        data.put(1, 3);
//        data.put(2, 10);
//        data.put(3, 100);
//        data.put(4, 12);
//       if( data.containsKey(10)){
//           System.out.println("contain");
//       }
//        if( data.containsKey(1)){
//            System.out.println("contain 1");
//        }
//        // 选择带宽
//        double bandwidth = 1.0;
//
//        // 计算核密度曲线的极小值点
//        findMinima(data, bandwidth);
//    }
//
//    static void findMinima(Map<Integer, Integer> data, double bandwidth) {
//        // 计算核密度估计
//        Map<Integer, Double> kernelDensityEstimate = calculateKernelDensity(data, bandwidth);
//
//        // 计算导数
//        Map<Integer, Double> derivative = calculateDerivative(kernelDensityEstimate);
//
//        System.out.println(derivative);
//
//        // 打印导数为零的点
//        System.out.println("Minima Points:");
//        for (Map.Entry<Integer, Double> entry : derivative.entrySet()) {
//            if (entry.getValue() == 0.0) {
//                System.out.println("Point " + entry.getKey());
//            }
//        }
//    }
//
//    private static Map<Integer, Double> calculateKernelDensity(Map<Integer, Integer> data, double bandwidth) {
//        // 计算核密度估计
//        Map<Integer, Double> kernelDensityEstimate = new HashMap<>();
//
//        for (Map.Entry<Integer, Integer> entry : data.entrySet()) {
//            int point = entry.getKey();
//            double sum = 0.0;
//
//            for (Map.Entry<Integer, Integer> dataEntry : data.entrySet()) {
//                double x = dataEntry.getKey();
//                double kernel = gaussianKernel(x, point, bandwidth);
//                sum += kernel;
//            }
//
//            kernelDensityEstimate.put(point, sum / (data.size() * bandwidth));
//        }
//
//        return kernelDensityEstimate;
//    }
//
//    private static Map<Integer, Double> calculateDerivative(Map<Integer, Double> function) {
//        // 计算导数
//        Map<Integer, Double> derivative = new HashMap<>();
//
//        for (Map.Entry<Integer, Double> entry : function.entrySet()) {
//            int point = entry.getKey();
//
//            if (point > 1 && point < 4) {
//                double derivativeValue = (function.get(point + 1) - function.get(point - 1)) / 2.0;
//                derivative.put(point, derivativeValue);
//            } else {
//                // 边缘点处理
//                derivative.put(point, 0.0);
//            }
//        }
//
//        return derivative;
//    }
//
//    private static double gaussianKernel(double x, double xi, double bandwidth) {
//        // 高斯核函数
//        return Math.exp(-0.5 * Math.pow((x - xi) / bandwidth, 2)) / Math.sqrt(2 * Math.PI);
//    }
}

