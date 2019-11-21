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
package org.apache.iotdb.db.conf.adapter;

public interface IActiveTimeSeriesCounter {

  /**
   * Initialize the counter by adding a new HyperLogLog counter for the given storage group
   *
   * @param storageGroup the given storage group to be initialized
   */
  void init(String storageGroup);

  /**
   * Register a time series to the active time series counter
   *
   * @param storageGroup the storage group name of the time series
   * @param device the device name of the time series
   * @param measurement the sensor name of the time series
   */
  void offer(String storageGroup, String device, String measurement);

  /**
   * Update the ActiveRatioMap
   *
   * @param storageGroup whose counter will be refreshed after the update
   */
  void updateActiveRatio(String storageGroup);

  /**
   * Get the active time series number proportion of the given storage group
   *
   * @param storageGroup the storage group to be calculated
   * @return the active time series number proportion of the given storage group
   */
  double getActiveRatio(String storageGroup);

  /**
   * Delete the counter for the given storage group
   *
   * @param storageGroup whose counter will be removed
   */
  void delete(String storageGroup);

}