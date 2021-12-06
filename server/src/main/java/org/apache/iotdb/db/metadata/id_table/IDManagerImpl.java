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

package org.apache.iotdb.db.metadata.id_table;

import com.sun.tools.javac.util.Pair;
import java.util.Map;
import org.apache.iotdb.db.metadata.id_table.entry.TimeseriesID;

public class IDManagerImpl implements IDManager {

  /** storage group name -> ID table */
  private Map<String, IDTable> storageGroupIDTableMap;

  /**
   * check whether a time series is exist if exist, check the type consistency if not exist, call
   * MManager to create it
   *
   * @param seriesKey full path of the time series
   * @return timeseries ID of this time series
   */
  @Override
  public TimeseriesID checkOrCreateIfNotExist(String seriesKey) {
    return null;
  }

  /**
   * upatde latest flushed time of one timeseries
   *
   * @param timeseriesID timeseries id
   * @param flushedTime latest flushed time
   */
  @Override
  public void updateLatestFlushedTime(TimeseriesID timeseriesID, long flushedTime) {}

  /**
   * upatde latest flushed time of one timeseries
   *
   * @param timeseriesID timeseries id
   * @return latest flushed time of one timeseries
   */
  @Override
  public long getLatestFlushedTime(TimeseriesID timeseriesID) {
    return 0;
  }

  /**
   * get latest time value pair of one timeseries
   *
   * @param timeseriesID timeseries id
   * @return latest time value pair of one timeseries
   */
  @Override
  public Pair<Long, Object> getLastTimeValuePair(TimeseriesID timeseriesID) {
    return null;
  }

  /**
   * update latest time value pair of one timeseries
   *
   * @param timeseriesID timeseries id
   * @param lastTimeValue latest time value pair of one timeseries
   */
  @Override
  public void updateLastTimeValuePair(
      TimeseriesID timeseriesID, Pair<Long, Object> lastTimeValue) {}
}
