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

import org.junit.Test;

import static org.junit.Assert.fail;

public class TimestampPrecisionUtilsTest {

  @Test
  public void testCheckMsTimestampPrecision() {
    TimestampPrecisionUtils.TIMESTAMP_PRECISION = "ms";
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(-1L);
      TimestampPrecisionUtils.checkTimestampPrecision(0L);
      TimestampPrecisionUtils.checkTimestampPrecision(1694689856546L);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckIllegalMsTimestampPrecision() {
    TimestampPrecisionUtils.TIMESTAMP_PRECISION = "ms";
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(1694689856546000L);
      fail();
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testCheckUsTimestampPrecision() {
    TimestampPrecisionUtils.TIMESTAMP_PRECISION = "us";
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(-1L);
      TimestampPrecisionUtils.checkTimestampPrecision(0L);
      TimestampPrecisionUtils.checkTimestampPrecision(1694689856546L);
      TimestampPrecisionUtils.checkTimestampPrecision(1694689856546000L);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void testCheckIllegalUsTimestampPrecision() {
    TimestampPrecisionUtils.TIMESTAMP_PRECISION = "us";
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(1694689856546000000L);
      fail();
    } catch (Exception ignored) {
    }
  }

  @Test
  public void testCheckNsTimestampPrecision() {
    TimestampPrecisionUtils.TIMESTAMP_PRECISION = "ns";
    try {
      TimestampPrecisionUtils.checkTimestampPrecision(-1L);
      TimestampPrecisionUtils.checkTimestampPrecision(0L);
      TimestampPrecisionUtils.checkTimestampPrecision(1694689856546L);
      TimestampPrecisionUtils.checkTimestampPrecision(1694689856546000L);
      TimestampPrecisionUtils.checkTimestampPrecision(1694689856546000000L);
      TimestampPrecisionUtils.checkTimestampPrecision(Long.MAX_VALUE);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
