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

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;

import java.util.concurrent.TimeUnit;

public class TimestampPrecisionUtils {
  public static String TIMESTAMP_PRECISION =
      CommonDescriptor.getInstance().getConfig().getTimestampPrecision();
  private static final boolean isTimestampPrecisionCheckEnabled =
      CommonDescriptor.getInstance().getConfig().isTimestampPrecisionCheckEnabled();

  @FunctionalInterface
  private interface ConvertFunction<T1, T2, R> {
    R apply(T1 t1, T2 t2);
  }

  private static final ConvertFunction<Long, TimeUnit, Long> convertToCurrPrecisionFunction;
  public static final TimeUnit currPrecision;

  static {
    switch (TIMESTAMP_PRECISION) {
      case "ms":
        convertToCurrPrecisionFunction = TimeUnit.MILLISECONDS::convert;
        currPrecision = TimeUnit.MILLISECONDS;
        break;
      case "us":
        convertToCurrPrecisionFunction = TimeUnit.MICROSECONDS::convert;
        currPrecision = TimeUnit.MICROSECONDS;
        break;
      case "ns":
        convertToCurrPrecisionFunction = TimeUnit.NANOSECONDS::convert;
        currPrecision = TimeUnit.NANOSECONDS;
        break;
        // this case will never reach
      default:
        throw new UnsupportedOperationException(
            "not supported time_precision: " + TIMESTAMP_PRECISION);
    }
  }

  /** convert specific precision timestamp to current precision timestamp */
  public static long convertToCurrPrecision(long sourceTime, TimeUnit sourceUnit) {
    return convertToCurrPrecisionFunction.apply(sourceTime, sourceUnit);
  }

  /** check whether the input timestamp match the current system timestamp precision. */
  public static void checkTimestampPrecision(long time) {
    if (!isTimestampPrecisionCheckEnabled) {
      return;
    }
    switch (TIMESTAMP_PRECISION) {
      case "ms":
        if (time > 10_000_000_000_000L) {
          throw new SemanticException(
              String.format(
                  "Current system timestamp precision is %s, "
                      + "please check whether the timestamp %s is correct.",
                  TIMESTAMP_PRECISION, time));
        }
        break;
      case "us":
        if (time > 10_000_000_000_000_000L) {
          throw new SemanticException(
              String.format(
                  "Current system timestamp precision is %s, "
                      + "please check whether the timestamp %s is correct.",
                  TIMESTAMP_PRECISION, time));
        }
        break;
        // Long.MaxValue is 19 digits, therefore no problem when the precision is ns.
      case "ns":
      default:
        break;
    }
  }
}
