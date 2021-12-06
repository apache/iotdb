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

import java.util.Map;
import org.apache.iotdb.db.metadata.id_table.entry.DeviceEntry;
import org.apache.iotdb.db.metadata.id_table.entry.IDeviceID;
import org.apache.iotdb.db.metadata.id_table.entry.TimeseriesID;

public class IDTable {

  /**
   * 256 hashmap for avoiding rehash performance issue and lock competition device ID ->
   * (measurement name -> schema entry)
   */
  private Map<IDeviceID, DeviceEntry>[] idTables;

  /** disk schema manager to manage disk schema entry */
  private DiskSchemaManager diskSchemaManager;

  /**
   * check whether a time series is exist if exist, check the type consistency if not exist, call
   * MManager to create it
   *
   * @param timeseriesID ID of the time series
   * @return type consistency
   */
  public boolean checkOrCreateIfNotExist(TimeseriesID timeseriesID) {
    return false;
  }

  /**
   * upatde latest flushed time of one timeseries
   *
   * @param timeseriesID timeseries id
   * @param flushedTime latest flushed time
   */
  public void updateLatestFlushedTime(TimeseriesID timeseriesID, long flushedTime) {}

  /**
   * upatde latest flushed time of one timeseries
   *
   * @param timeseriesID timeseries id
   * @return latest flushed time of one timeseries
   */
  public long getLatestFlushedTime(TimeseriesID timeseriesID) {
    return 0;
  }
}
