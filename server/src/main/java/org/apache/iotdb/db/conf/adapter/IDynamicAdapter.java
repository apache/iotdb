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

import org.apache.iotdb.db.exception.ConfigAdjusterException;

public interface IDynamicAdapter {

  /**
   * Adjust parameters of maxMemtableNumber, memtableSizeThreshold and tsFileSizeThreshold.
   * @return {@code true} if it has appropriate parameters and adjust them successfully
   *         {@code false} adjusting parameters failed
   */
  boolean tryToAdaptParameters();

  /**
   * Add or delete storage groups
   *
   * @param diff it's positive if add new storage groups; it's negative if delete old storage
   * groups.
   */
  void addOrDeleteStorageGroup(int diff) throws ConfigAdjusterException;

  /**
   * Add or delete timeseries
   *
   * @param diff it's positive if create new timeseries; it's negative if delete old timeseris.
   */
  void addOrDeleteTimeSeries(int diff) throws ConfigAdjusterException;

}
