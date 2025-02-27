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

package org.apache.iotdb.db.utils;

import org.apache.tsfile.common.conf.TSFileDescriptor;

import java.util.List;

public class MathUtils {

  private MathUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static float roundWithGivenPrecision(float data, int size) {
    if (Float.isNaN(data) || data > Integer.MAX_VALUE || data < Integer.MIN_VALUE) {
      return data;
    }
    if (size == 0) {
      return Math.round(data);
    }
    return Math.round(data)
        + Math.round(((data - Math.round(data)) * Math.pow(10, size))) / (float) Math.pow(10, size);
  }

  public static float roundWithGivenPrecision(float data) {
    if (Float.isNaN(data) || data > Integer.MAX_VALUE || data < Integer.MIN_VALUE) {
      return data;
    }
    if (TSFileDescriptor.getInstance().getConfig().getFloatPrecision() == 0) {
      return Math.round(data);
    }
    return Math.round(data)
        + Math.round(
                ((data - Math.round(data))
                    * (float)
                        Math.pow(
                            10, TSFileDescriptor.getInstance().getConfig().getFloatPrecision())))
            / (float) Math.pow(10, TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
  }

  public static double roundWithGivenPrecision(double data, int size) {
    if (Double.isNaN(data) || data > Long.MAX_VALUE || data < Long.MIN_VALUE) {
      return data;
    }
    if (size == 0) {
      return Math.round(data);
    }
    return Math.round(data)
        + Math.round(((data - Math.round(data)) * Math.pow(10, size))) / Math.pow(10, size);
  }

  public static double roundWithGivenPrecision(double data) {
    if (Double.isNaN(data) || data > Long.MAX_VALUE || data < Long.MIN_VALUE) {
      return data;
    }
    if (TSFileDescriptor.getInstance().getConfig().getFloatPrecision() == 0) {
      return Math.round(data);
    }
    return Math.round(data)
        + Math.round(
                ((data - Math.round(data))
                    * Math.pow(10, TSFileDescriptor.getInstance().getConfig().getFloatPrecision())))
            / Math.pow(10, TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
  }

  /**
   * calculate sum of list
   *
   * @param data need to calculate list
   * @return sum of list
   */
  public static double sum(List<Double> data) {
    double sum = 0;
    for (Double datum : data) {
      sum = sum + datum;
    }
    return sum;
  }

  /**
   * calculate mean of list
   *
   * @param data need to calculate list
   * @return mean of list
   */
  public static double mean(List<Double> data) {
    return sum(data) / data.size();
  }

  /**
   * calculate pop variance of list
   *
   * @param data need to calculate list
   * @return pop variance of list
   */
  public static double popVariance(List<Double> data) {
    double variance = 0;
    for (int i = 0; i < data.size(); i++) {
      variance = variance + (Math.pow((data.get(i) - mean(data)), 2));
    }
    variance = variance / data.size();
    return variance;
  }

  /**
   * calculate pop std dev of list
   *
   * @param data need to calculate list
   * @return pop std dev of list
   */
  public static double popStdDev(List<Double> data) {
    return Math.sqrt(popVariance(data));
  }
}
