/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.jdbc;

import java.sql.SQLException;
import java.time.ZoneId;
import java.util.List;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSIService.Iface;
import org.apache.iotdb.service.rpc.thrift.TSInsertionReq;
import org.apache.iotdb.service.rpc.thrift.TS_SessionHandle;
import org.apache.thrift.TException;

public class IoTDBPreparedInsertionStatement extends IoTDBPreparedStatement {

  private static final int SUCCESS_CODE = 200;
  private TSInsertionReq req = new TSInsertionReq();

  public IoTDBPreparedInsertionStatement(IoTDBConnection connection,
      Iface client,
      TS_SessionHandle sessionHandle, ZoneId zoneId) throws SQLException {
    super(connection, client, sessionHandle, zoneId);
    req.setStmtId(stmtId);
  }

  @Override
  public boolean execute() throws SQLException {

    try {
      TSExecuteStatementResp resp = client.insert(req);
      req.unsetDeviceId();
      req.unsetMeasurements();
      req.unsetTimestamp();
      req.unsetValues();
      return resp.getStatus().getStatusType().getCode() == SUCCESS_CODE;
    } catch (TException e) {
      throw new SQLException(e);
    }
  }

  public void setTimestamp(long timestamp) {
    req.setTimestamp(timestamp);
  }

  public void setDeviceId(String deviceId) {
    req.setDeviceId(deviceId);
  }

  public void setMeasurements(List<String> measurements) {
    req.setMeasurements(measurements);
  }

  public void setValues(List<String> values) {
    req.setValues(values);
  }
}
