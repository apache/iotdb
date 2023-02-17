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
package org.apache.iotdb.external.api;

import java.util.Properties;

/** An interface for series number monitoring, users can implement their own limitation strategy */
public interface ISeriesNumerMonitor {

  /**
   * do the necessary initialization
   *
   * @param properties Properties containing all the parameters needed to init
   */
  void init(Properties properties);
  /**
   * add time series
   *
   * @param number time series number for current createTimeSeries operation
   * @return true if we want to allow the operation, otherwise false
   */
  boolean addTimeSeries(int number);

  /**
   * delete time series
   *
   * @param number time series number for current deleteTimeSeries operation
   */
  void deleteTimeSeries(int number);
}
