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

package org.apache.iotdb.confignode.manager.load.cache;

/**
 * AbstractStatistics contains the statistics nano timestamp when calculating the statistics. All
 * kinds of statistics should extend this class then expand its required fields.
 */
public abstract class AbstractStatistics {

  // The nano timestamp when the statistics is generated
  private final long statisticsNanoTimestamp;

  protected AbstractStatistics(long statisticsNanoTimestamp) {
    this.statisticsNanoTimestamp = statisticsNanoTimestamp;
  }

  public long getStatisticsNanoTimestamp() {
    return statisticsNanoTimestamp;
  }

  /**
   * Compare if this statistics is newer than the given one.
   *
   * @param o The given statistics.
   * @return True if the logical timestamp of this statistics is newer than the given one, False
   *     otherwise.
   */
  public boolean isNewerThan(AbstractStatistics o) {
    return statisticsNanoTimestamp > o.getStatisticsNanoTimestamp();
  }
}
