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

package org.apache.iotdb.db.protocol.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.db.protocol.client.ainode.AINodeClient;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.util.Optional;

/**
 * A dedicated Thrift client factory for AINodeClient. This removes AINode pooling/creation from the
 * generic ClientPoolFactory to avoid cross-node client mixing.
 */
public class AINodeClientFactory extends ThriftClientFactory<TEndPoint, AINodeClient> {

  public AINodeClientFactory(
      ClientManager<TEndPoint, AINodeClient> clientManager,
      ThriftClientProperty thriftClientProperty) {
    super(clientManager, thriftClientProperty);
  }

  @Override
  public PooledObject<AINodeClient> makeObject(TEndPoint endPoint) throws Exception {
    return new DefaultPooledObject<>(
        new AINodeClient(thriftClientProperty, endPoint, clientManager));
  }

  @Override
  public void destroyObject(TEndPoint key, PooledObject<AINodeClient> pooled) throws Exception {
    pooled.getObject().invalidate();
  }

  @Override
  public boolean validateObject(TEndPoint key, PooledObject<AINodeClient> pooledObject) {
    return Optional.ofNullable(pooledObject.getObject().getTransport())
        .map(org.apache.thrift.transport.TTransport::isOpen)
        .orElse(false);
  }
}
