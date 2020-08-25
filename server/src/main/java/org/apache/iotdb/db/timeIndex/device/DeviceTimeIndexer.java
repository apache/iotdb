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
package org.apache.iotdb.db.timeIndex.device;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.List;
import java.util.Map;

/**
 * manage the index [deviceId, (startTime, endTime, TsFilePath)] for all devices
 */
public interface DeviceTimeIndexer {
  /**
   * init the Indexer when IoTDB start
   * @return whether success
   */
  public boolean init();

  /**
   * may do some prepared work before operation
   * @return whether success
   */
  public boolean begin();

  /**
   * may do some resource release after operation
   * @return whether success
   */
  public boolean end();

  /**
   * add one index for the deviceId
   * @param deviceId
   * @param startTime
   * @param endTime
   * @param tsFilePath
   * @return whether success
   */
  public boolean addIndexForDevice(PartialPath deviceId, long startTime, long endTime, String tsFilePath);

  /**
   * add one index for the deviceId
   * @param deviceId
   * @param startTime
   * @param endTime
   * @param tsFilePath
   * @return
   */
  public boolean addIndexForDevice(String deviceId, long startTime, long endTime, String tsFilePath);

  /**
   * add all indexs for a flushed tsFile
   * @param deviceIds
   * @param startTimes
   * @param endTimes
   * @param tsFilePath
   * @return
   */
  public boolean addIndexForDevices(Map<String, Integer> deviceIds, long[] startTimes, long[] endTimes, String tsFilePath);

  /**
   * delete one index for the deviceId
   * @param deviceId
   * @param startTime
   * @param endTime
   * @param tsFilePath
   * @return whether success
   */
  public boolean deleteIndexForDevice(PartialPath deviceId, long startTime, long endTime, String tsFilePath);

  /**
   * delete one index for the deviceId
   * @param deviceId
   * @param startTime
   * @param endTime
   * @param tsFilePath
   * @return
   */
  public boolean deleteIndexForDevice(String deviceId, long startTime, long endTime, String tsFilePath);

  /**
   * delete all index for one deleted tsfile
   * @param deviceIds
   * @param startTimes
   * @param endTimes
   * @param tsFilePath
   * @return
   */
  public boolean deleteIndexForDevices(Map<String, Integer> deviceIds, long[] startTimes, long[] endTimes, String tsFilePath);

  /**
   * after merge, we should keep the index is updated in consistency
   * @param updateIndexsParam
   * @return whether success
   */
  public boolean updateIndexForDevices(UpdateIndexsParam updateIndexsParam);

  /**
   * found the related tsFile(only cover sealed tsfile) for one deviceId
   * @param deviceId
   * @param timeFilter
   * @return whether success
   */
  public List<TsFileResource> filterByOneDevice(PartialPath deviceId, Filter timeFilter);
}
