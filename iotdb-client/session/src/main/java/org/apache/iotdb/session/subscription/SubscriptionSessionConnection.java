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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConnection;

import org.apache.thrift.TException;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class SubscriptionSessionConnection extends SessionConnection {

  private static final String SHOW_DATA_NODES_COMMAND = "SHOW DATANODES";
  private static final String NODE_ID_COLUMN_NAME = "NodeID";
  private static final String STATUS_COLUMN_NAME = "Status";
  private static final String IP_COLUMN_NAME = "RpcAddress";
  private static final String PORT_COLUMN_NAME = "RpcPort";
  private static final String REMOVING_STATUS = "Removing";

  public SubscriptionSessionConnection(
      Session session,
      TEndPoint endPoint,
      ZoneId zoneId,
      Supplier<List<TEndPoint>> availableNodes,
      int maxRetryCount,
      long retryIntervalInMs,
      String sqlDialect,
      String database)
      throws IoTDBConnectionException {
    super(
        session,
        endPoint,
        zoneId,
        availableNodes,
        maxRetryCount,
        retryIntervalInMs,
        sqlDialect,
        database);
  }

  // from org.apache.iotdb.session.NodesSupplier.updateDataNodeList
  public Map<Integer, TEndPoint> fetchAllEndPoints()
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement(SHOW_DATA_NODES_COMMAND);
    SessionDataSet.DataIterator iterator = dataSet.iterator();
    Map<Integer, TEndPoint> endPoints = new HashMap<>();
    while (iterator.next()) {
      // ignore removing DN
      if (REMOVING_STATUS.equals(iterator.getString(STATUS_COLUMN_NAME))) {
        continue;
      }
      String ip = iterator.getString(IP_COLUMN_NAME);
      String port = iterator.getString(PORT_COLUMN_NAME);
      if (ip != null && port != null) {
        endPoints.put(
            iterator.getInt(NODE_ID_COLUMN_NAME), new TEndPoint(ip, Integer.parseInt(port)));
      }
    }
    return endPoints;
  }

  public TPipeSubscribeResp pipeSubscribe(final TPipeSubscribeReq req) throws TException {
    return client.pipeSubscribe(req);
  }
}
