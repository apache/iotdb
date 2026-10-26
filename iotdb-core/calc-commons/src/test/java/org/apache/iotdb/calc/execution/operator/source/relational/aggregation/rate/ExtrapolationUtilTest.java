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

package org.apache.iotdb.calc.execution.operator.source.relational.aggregation.rate;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.queryengine.utils.TimestampPrecisionUtils;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class ExtrapolationUtilTest {

  private static final String TEST_TIMESTAMP_PRECISION_PROPERTY = "rate.test.timestamp_precision";

  @BeforeClass
  public static void setUpTimestampPrecision() {
    String precision = System.getProperty(TEST_TIMESTAMP_PRECISION_PROPERTY, "ms");
    CommonDescriptor.getInstance().getConfig().setTimestampPrecision(precision);
    assertEquals(toTimeUnit(precision), TimestampPrecisionUtils.currPrecision);
  }

  @Test
  public void testThresholdEquality() {
    assertEquals(25.0, ExtrapolationUtil.extrapolate(2, 11, 100.0, 21, 0, 31, 10.0, false), 1E-12);
  }

  @Test
  public void testOneTickBelowThreshold() {
    assertEquals(30.0, ExtrapolationUtil.extrapolate(2, 10, 100.0, 20, 0, 30, 10.0, false), 1E-12);
  }

  private static TimeUnit toTimeUnit(String precision) {
    switch (precision) {
      case "ms":
        return TimeUnit.MILLISECONDS;
      case "us":
        return TimeUnit.MICROSECONDS;
      case "ns":
        return TimeUnit.NANOSECONDS;
      default:
        throw new IllegalArgumentException(precision);
    }
  }
}
