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

package org.apache.iotdb.db.mpp.statistics;

import org.apache.iotdb.commons.path.PartialPath;

import com.google.common.collect.Maps;

import java.util.Map;

public class StatisticsManager {

  private final Map<PartialPath, TimeseriesStats> seriesToStatsMap = Maps.newConcurrentMap();

  public long getMaxBinarySizeInBytes(PartialPath path) {
    return 512L * Byte.BYTES;
  }

  public static StatisticsManager getInstance() {
    return StatisticsManager.StatisticsManagerHelper.INSTANCE;
  }

  private static class StatisticsManagerHelper {

    private static final StatisticsManager INSTANCE = new StatisticsManager();

    private StatisticsManagerHelper() {}
  }
}
