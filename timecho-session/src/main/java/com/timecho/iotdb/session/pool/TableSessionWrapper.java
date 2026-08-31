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

package com.timecho.iotdb.session.pool;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.pool.SessionPool;

import com.timecho.iotdb.isession.ITableSession;
import com.timecho.iotdb.session.TableSession;

import java.util.List;

public class TableSessionWrapper extends org.apache.iotdb.session.pool.TableSessionWrapper
    implements ITableSession {

  protected TableSessionWrapper(Session session, SessionPool sessionPool) {
    super(session, sessionPool);
  }

  @Override
  public String getDeviceLeaderURL(
      String dbName, List<String> deviceId, List<Boolean> isSetTag, long time)
      throws IoTDBConnectionException, StatementExecutionException {
    return TableSession.getDeviceLeaderURL(dbName, deviceId, isSetTag, time, session);
  }

  @Override
  public String getDeviceLeaderURL(
      String dbName,
      List<String> deviceId,
      List<Boolean> isSetTag,
      List<String> tagColumnNames,
      long time)
      throws IoTDBConnectionException, StatementExecutionException {
    return TableSession.getDeviceLeaderURL(
        dbName, deviceId, isSetTag, tagColumnNames, time, session);
  }
}
