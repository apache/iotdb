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
package org.apache.iotdb.db.protocol.influxdb.meta;

import java.util.Map;
import java.util.Set;

/** used to manage influxdb metadata */
public interface IInfluxDBMetaManager {

  /** recover the influxdb metadata */
  void recover();

  /**
   * get field orders
   *
   * @param database database of influxdb
   * @param measurement measurement of influxdb
   * @param sessionId session id
   * @return a map of field orders
   */
  Map<String, Integer> getFieldOrders(String database, String measurement, long sessionId);

  /**
   * generate time series path for insertion
   *
   * @param database database of influxdb
   * @param measurement measurement of influxdb
   * @param tags influxdb tags
   * @param fields influxdb fields
   * @param sessionID session id
   * @return series path
   */
  String generatePath(
      String database,
      String measurement,
      Map<String, String> tags,
      Set<String> fields,
      long sessionID);

  /**
   * get tag orders
   *
   * @param database database of influxdb
   * @param measurement measurement of influxdb
   * @param sessionID session id
   * @return a map of tag orders
   */
  Map<String, Integer> getTagOrders(String database, String measurement, long sessionID);
}
