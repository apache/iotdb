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

package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.commons.client.BaseClientFactory;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientPoolProperty;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientRpc;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftGroup;

public class RatisClientFactory extends BaseClientFactory<RaftGroup, RaftClient> {

  private final RaftProperties raftProperties;
  private final RaftClientRpc clientRpc;

  public RatisClientFactory(
      ClientManager<RaftGroup, RaftClient> clientManager,
      ClientPoolProperty<RaftClient> clientManagerProperty,
      RaftProperties raftProperties,
      RaftClientRpc clientRpc) {
    super(clientManager, clientManagerProperty);
    this.raftProperties = raftProperties;
    this.clientRpc = clientRpc;
  }

  @Override
  public void destroyObject(RaftGroup key, PooledObject<RaftClient> pooledObject) throws Exception {
    pooledObject.getObject().close();
  }

  @Override
  public PooledObject<RaftClient> makeObject(RaftGroup group) throws Exception {
    return new DefaultPooledObject<>(
        RaftClient.newBuilder()
            .setProperties(raftProperties)
            .setRaftGroup(group)
            .setClientRpc(clientRpc)
            .build());
  }

  @Override
  public boolean validateObject(RaftGroup key, PooledObject<RaftClient> pooledObject) {
    return true;
  }
}
