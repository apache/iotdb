/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.monitor;

import org.apache.iotdb.tsfile.write.record.TSRecord;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public interface IStatistic {

  /**
   * Get A HashMap that contains the module seriesPaths and their statistics measurement.
   *
   * @return A HashMap that contains the module seriesPath like: root.stats.write.global, and its
   *     value is TSRecord format contains all statistics measurement
   */
  Map<String, TSRecord> getAllStatisticsValue();

  /** registerStatMetadata registers statistics info to the manager. */
  void registerStatMetadata();

  /**
   * Get all module's statistics parameters as a time-series seriesPath.
   *
   * @return a list of string like "root.stats.xxx.statisticsParams",
   */
  List<String> getAllPathForStatistic();

  /**
   * Get a HashMap contains the names and values of the statistics parameters.
   *
   * @return a HashMap contains the names and values of the statistics parameters
   */
  Map<String, AtomicLong> getStatParamsHashMap();
}
