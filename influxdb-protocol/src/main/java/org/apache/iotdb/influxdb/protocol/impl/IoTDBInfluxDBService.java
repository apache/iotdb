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

package org.apache.iotdb.influxdb.protocol.impl;

import org.apache.iotdb.influxdb.session.InfluxDBSession;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSCreateDatabaseReq;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.TSWritePointsReq;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.influxdb.InfluxDBException;

public class IoTDBInfluxDBService {

  private String currentDatabase;

  private final InfluxDBSession influxDBSession;

  public IoTDBInfluxDBService(String host, int rpcPort, String username, String password) {
    influxDBSession = new InfluxDBSession(host, rpcPort, username, password);
    try {
      influxDBSession.open();
    } catch (IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  public void writePoints(
      String database,
      String retentionPolicy,
      String precision,
      String consistency,
      String lineProtocol) {
    TSWritePointsReq tsWritePointsReq = new TSWritePointsReq();
    if (database == null) {
      tsWritePointsReq.setDatabase(currentDatabase);
    } else {
      tsWritePointsReq.setDatabase(database);
    }
    tsWritePointsReq.setRetentionPolicy(retentionPolicy);
    tsWritePointsReq.setPrecision(precision);
    tsWritePointsReq.setConsistency(consistency);
    tsWritePointsReq.setLineProtocol(lineProtocol);
    try {
      influxDBSession.writePoints(tsWritePointsReq);
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  public void setDatabase(String database) {
    currentDatabase = database;
  }

  public void createDatabase(String database) {
    TSCreateDatabaseReq tsCreateDatabaseReq = new TSCreateDatabaseReq();
    tsCreateDatabaseReq.setDatabase(database);
    try {
      influxDBSession.createDatabase(tsCreateDatabaseReq);
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  public void close() {
    influxDBSession.close();
  }
}
