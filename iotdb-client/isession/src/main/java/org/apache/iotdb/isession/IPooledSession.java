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

package org.apache.iotdb.isession;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;

import org.apache.thrift.TException;
import org.apache.tsfile.write.record.Tablet;

public interface IPooledSession extends AutoCloseable {

  Version getVersion();

  int getFetchSize();

  void close() throws IoTDBConnectionException;

  String getTimeZone();

  SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException;

  void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException;

  String getTimestampPrecision() throws TException;

  void insertTablet(Tablet tablet) throws StatementExecutionException, IoTDBConnectionException;

  boolean isEnableQueryRedirection();

  boolean isEnableRedirection();

  TSBackupConfigurationResp getBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException;

  TSConnectionInfoResp fetchAllConnections() throws IoTDBConnectionException;

  long getQueryTimeout();
}
