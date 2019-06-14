/**
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

package org.apache.iotdb.db.cost.statistic;

public interface MeasurementMBean {

  /**
   * start display performance statistic every interval of displayIntervalInMs.
   */
  void startContinuousStatistics();

  /**
   * start display performance statistic after interval of displayIntervalInMs.
   */
  void startOneTimeStatistics();

  /**
   * stop display performance statistic.
   */
  void stopStatistic();

  /**
   * clear current stat result, reset statistical state.
   */
  void clearStatisticalState();

  boolean isEnableStat();

  long getDisplayIntervalInMs();

  void setDisplayIntervalInMs(long displayIntervalInMs);
}
