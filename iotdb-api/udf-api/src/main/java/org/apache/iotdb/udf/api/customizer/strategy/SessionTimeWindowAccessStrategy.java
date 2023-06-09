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

package org.apache.iotdb.udf.api.customizer.strategy;

import java.time.ZoneId;

public class SessionTimeWindowAccessStrategy implements AccessStrategy {

  private final long displayWindowBegin;
  private final long displayWindowEnd;
  private final long sessionTimeGap;

  private ZoneId zoneId;

  /**
   * @param displayWindowBegin displayWindowBegin < displayWindowEnd
   * @param displayWindowEnd displayWindowBegin < displayWindowEnd
   * @param sessionTimeGap 0 <= sessionTimeGap
   */
  public SessionTimeWindowAccessStrategy(
      long displayWindowBegin, long displayWindowEnd, long sessionTimeGap) {
    this.displayWindowBegin = displayWindowBegin;
    this.displayWindowEnd = displayWindowEnd;
    this.sessionTimeGap = sessionTimeGap;
  }

  /**
   * Display window begin will be set to the same as the minimum timestamp of the query result set,
   * and display window end will be set to the same as the maximum timestamp of the query result
   * set.
   *
   * @param sessionTimeGap 0 <= sessionTimeGap
   */
  public SessionTimeWindowAccessStrategy(long sessionTimeGap) {
    this.displayWindowBegin = Long.MIN_VALUE;
    this.displayWindowEnd = Long.MAX_VALUE;
    this.sessionTimeGap = sessionTimeGap;
  }

  @Override
  public void check() {
    if (sessionTimeGap < 0) {
      throw new RuntimeException(
          String.format(
              "Parameter sessionTimeGap(%d) should be equal to or greater than zero.",
              sessionTimeGap));
    }
    if (displayWindowEnd < displayWindowBegin) {
      throw new RuntimeException(
          String.format(
              "displayWindowEnd(%d) < displayWindowBegin(%d)",
              displayWindowEnd, displayWindowBegin));
    }
  }

  @Override
  public AccessStrategyType getAccessStrategyType() {
    return AccessStrategyType.SESSION_TIME_WINDOW;
  }

  public long getDisplayWindowBegin() {
    return displayWindowBegin;
  }

  public long getDisplayWindowEnd() {
    return displayWindowEnd;
  }

  public long getSessionTimeGap() {
    return sessionTimeGap;
  }

  public ZoneId getZoneId() {
    return zoneId;
  }

  public void setZoneId(ZoneId zoneId) {
    this.zoneId = zoneId;
  }
}
