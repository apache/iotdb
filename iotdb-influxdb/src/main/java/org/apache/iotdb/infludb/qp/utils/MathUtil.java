/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.infludb.qp.utils;

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
