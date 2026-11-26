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

package org.apache.iotdb.db.protocol.client.an;

import org.apache.iotdb.ainode.rpc.thrift.TForecastReq;
import org.apache.iotdb.ainode.rpc.thrift.TForecastResp;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.queryengine.plan.relational.function.ITableFunctionAINodeService;
import org.apache.iotdb.db.exception.ainode.AINodeConnectionException;

import org.apache.thrift.TException;

public class DataNodeTableFunctionAINodeService implements ITableFunctionAINodeService {

  private static final IClientManager<Integer, AINodeClient> CLIENT_MANAGER =
      AINodeClientManager.getInstance();

  public static final DataNodeTableFunctionAINodeService INSTANCE =
      new DataNodeTableFunctionAINodeService();

  private DataNodeTableFunctionAINodeService() {}

  @Override
  public TForecastResp forecast(TForecastReq req) {
    try (AINodeClient client =
        CLIENT_MANAGER.borrowClient(AINodeClientManager.AINODE_ID_PLACEHOLDER)) {
      return client.forecast(req);
    } catch (ClientManagerException | TException e) {
      throw new AINodeConnectionException(e);
    }
  }
}
