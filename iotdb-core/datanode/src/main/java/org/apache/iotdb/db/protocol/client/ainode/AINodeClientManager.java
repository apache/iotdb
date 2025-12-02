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

package org.apache.iotdb.db.protocol.client.ainode;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.db.protocol.client.AINodeClientFactory;

public class AINodeClientManager {

  public static final int DEFAULT_AINODE_ID = 0;

  private static final AINodeClientManager INSTANCE = new AINodeClientManager();

  private final IClientManager<TEndPoint, AINodeClient> clientManager;

  private volatile TEndPoint defaultAINodeEndPoint;

  private AINodeClientManager() {
    this.clientManager =
        new IClientManager.Factory<TEndPoint, AINodeClient>()
            .createClientManager(new AINodeClientFactory.AINodeClientPoolFactory());
  }

  public static AINodeClientManager getInstance() {
    return INSTANCE;
  }

  public void updateDefaultAINodeLocation(TEndPoint endPoint) {
    this.defaultAINodeEndPoint = endPoint;
  }

  public AINodeClient borrowClient(TEndPoint endPoint) throws Exception {
    return clientManager.borrowClient(endPoint);
  }

  public AINodeClient borrowClient(int aiNodeId) throws Exception {
    if (aiNodeId != DEFAULT_AINODE_ID) {
      throw new IllegalArgumentException("Unsupported AINodeId: " + aiNodeId);
    }
    if (defaultAINodeEndPoint == null) {
      defaultAINodeEndPoint = AINodeClient.getCurrentEndpoint();
    }
    return clientManager.borrowClient(defaultAINodeEndPoint);
  }

  public void clear(TEndPoint endPoint) {
    clientManager.clear(endPoint);
  }

  public void clearAll() {
    clientManager.close();
  }

  public IClientManager<TEndPoint, AINodeClient> getRawClientManager() {
    return clientManager;
  }
}
