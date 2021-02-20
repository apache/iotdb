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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;

public class MathUtils {

  private MathUtils() {
    throw new IllegalStateException("Utility class");
  }
  /**
   * @param data data should be less than Long.MAX_VALUE. otherwise Math.round() will return wrong
   *     value.
   */
  public static float roundWithGivenPrecision(float data, int size) {
    if (size == 0) {
      return Math.round(data);
    }
    return Math.round(data)
        + Math.round(((data - Math.round(data)) * Math.pow(10, size))) / (float) Math.pow(10, size);
  }

  public static float roundWithGivenPrecision(float data) {
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

  /**
   * @param data data should be less than Long.MAX_VALUE. otherwise Math.round() will return wrong
   *     value.
   */
  public static double roundWithGivenPrecision(double data, int size) {
    if (size == 0) {
      return Math.round(data);
    }
    return Math.round(data)
        + Math.round(((data - Math.round(data)) * Math.pow(10, size))) / Math.pow(10, size);
  }

  /**
   * @param data data should be less than Long.MAX_VALUE. otherwise Math.round() will return wrong
   *     value.
   */
  public static double roundWithGivenPrecision(double data) {
    if (TSFileDescriptor.getInstance().getConfig().getFloatPrecision() == 0) {
      return Math.round(data);
    }
    return Math.round(data)
        + Math.round(
                ((data - Math.round(data))
                    * Math.pow(10, TSFileDescriptor.getInstance().getConfig().getFloatPrecision())))
            / Math.pow(10, TSFileDescriptor.getInstance().getConfig().getFloatPrecision());
  }
}
