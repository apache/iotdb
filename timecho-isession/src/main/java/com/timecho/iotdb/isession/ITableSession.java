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

package com.timecho.iotdb.isession;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import java.util.List;

public interface ITableSession extends org.apache.iotdb.isession.ITableSession {

  /**
   * Retrieves the DataNode URL of the device leader for a given database and a deviceID.
   *
   * <p>For writable views, the TAG values in {@code deviceId} must follow the source table's {@code
   * DESC} order. This overload does not inspect TAG column names or reorder values.
   *
   * @param dbName the name of the database.
   * @param deviceId a list of string for constructing the specified deviceID.
   * @param isSetTag a true indicating the deviceID is set, false otherwise.
   * @param time the time at which partition the device leader is queried.
   * @return the DataNode URL <ip:port> of the device leader as a String.
   */
  String getDeviceLeaderURL(String dbName, List<String> deviceId, List<Boolean> isSetTag, long time)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * Retrieves the DataNode URL of the device leader while identifying each supplied TAG value by
   * its column name.
   *
   * <p>{@code tagColumnNames} must contain one name for every {@code true} TAG entry in {@code
   * isSetTag}, in the same order as those values occur in {@code deviceId} (the table name is not
   * included). The server resolves writable-view aliases and arranges the values in the source
   * table's {@code DESC} order.
   *
   * @param dbName the name of the database.
   * @param deviceId a list containing the table name followed by values for set TAG columns.
   * @param isSetTag a true value indicates that the corresponding table/TAG segment is set.
   * @param tagColumnNames names of the supplied TAG columns, excluding the table name.
   * @param time the time at which partition the device leader is queried.
   * @return the DataNode URL {@code <ip:port>} of the device leader as a String.
   */
  String getDeviceLeaderURL(
      String dbName,
      List<String> deviceId,
      List<Boolean> isSetTag,
      List<String> tagColumnNames,
      long time)
      throws IoTDBConnectionException, StatementExecutionException;
}
