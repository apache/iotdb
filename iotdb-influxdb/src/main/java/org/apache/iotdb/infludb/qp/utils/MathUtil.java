package org.apache.iotdb.infludb.qp.utils;

import java.util.ArrayList;
import java.util.List;

public class MathUtil {
    /**
     * 计算double列表的和
     *
     * @param data 需要计算的列表
     * @return 列表之和
     */
    public static double Sum(List<Double> data) {
        double sum = 0;
        for (int i = 0; i < data.size(); i++)
            sum = sum + data.get(i);
        return sum;
    }

    /**
     * 计算double列表的平均值
     *
     * @param data 需要计算的列表
     * @return 列表平均值
     */
    public static double Mean(List<Double> data) {
        double mean = 0;
        mean = Sum(data) / data.size();
        return mean;
    }


    /**
     * 计算double列表的总体方差
     *
     * @param data 需要计算的列表
     * @return 列表总体方差
     */
    public static double POP_Variance(List<Double> data) {
        double variance = 0;
        for (int i = 0; i < data.size(); i++) {
            variance = variance + (Math.pow((data.get(i) - Mean(data)), 2));
        }
        variance = variance / data.size();
        return variance;
    }


    /**
     * 计算double列表的总体标准差
     *
     * @param data 需要计算的列表
     * @return 列表总体标准差
     */
    public static double POP_STD_dev(List<Double> data) {
        double std_dev;
        std_dev = Math.sqrt(POP_Variance(data));
        return std_dev;
    }


}
