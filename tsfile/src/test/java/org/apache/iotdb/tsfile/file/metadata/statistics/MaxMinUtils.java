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
package org.apache.iotdb.tsfile.file.metadata.statistics;

/** @author Yuyuan Kang */
public class MaxMinUtils {

  public static int maxInt(int[] array, int start, int length) {
    int ret = array[start];
    for (int i = 1; i < length; i++) {
      if (ret < array[start + i]) {
        ret = array[start + i];
      }
    }
    return ret;
  }

  public static int minInt(int[] array, int start, int length) {
    int ret = array[start];
    for (int i = 1; i < length; i++) {
      if (ret > array[start + i]) {
        ret = array[start + i];
      }
    }
    return ret;
  }

  public static double minDouble(double[] array, int start, int length) {
    double ret = array[start];
    for (int i = 1; i < length; i++) {
      if (ret > array[start + i]) {
        ret = array[start + i];
      }
    }
    return ret;
  }

  public static double maxDouble(double[] array, int start, int length) {
    double ret = array[start];
    for (int i = 1; i < length; i++) {
      if (ret < array[start + i]) {
        ret = array[start + i];
      }
    }
    return ret;
  }

  public static float minFloat(float[] array, int start, int length) {
    float ret = array[start];
    for (int i = 1; i < length; i++) {
      if (ret > array[start + i]) {
        ret = array[start + i];
      }
    }
    return ret;
  }

  public static float maxFloat(float[] array, int start, int length) {
    float ret = array[start];
    for (int i = 1; i < length; i++) {
      if (ret < array[start + i]) {
        ret = array[start + i];
      }
    }
    return ret;
  }

  public static long maxLong(long[] array, int start, int length) {
    long ret = array[start];
    for (int i = 1; i < length; i++) {
      if (ret < array[start + i]) {
        ret = array[start + i];
      }
    }
    return ret;
  }

  public static long minLong(long[] array, int start, int length) {
    long ret = array[start];
    for (int i = 1; i < length; i++) {
      if (ret > array[start + i]) {
        ret = array[start + i];
      }
    }
    return ret;
  }
}
