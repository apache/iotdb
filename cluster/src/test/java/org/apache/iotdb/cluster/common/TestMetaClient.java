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

package org.apache.iotdb.cluster.common;

import java.io.IOException;
import org.apache.iotdb.cluster.client.ClientPool;
import org.apache.iotdb.cluster.client.MetaClient;
import org.apache.iotdb.cluster.config.ClusterConstant;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusRequest;
import org.apache.iotdb.cluster.rpc.thrift.CheckStatusResponse;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;

public class TestMetaClient extends MetaClient {

  private Node node;

  public TestMetaClient(TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      Node node, ClientPool pool)
      throws IOException {
    super(protocolFactory, clientManager, node, pool);
    this.node = node;
  }

  public Node getNode() {
    return node;
  }

  public void setNode(Node node) {
    this.node = node;
  }

  @Override
  public void checkStatus(CheckStatusRequest status,
      AsyncMethodCallback<CheckStatusResponse> resultHandler) throws TException {
    long partitionInterval = status.getPartitionInterval();
    int hashSalt = status.getHashSalt();
    int replicationNum = status.getReplicationNumber();
    boolean partitionIntervalEquals = true;
    boolean hashSaltEquals = true;
    boolean replicationNumEquals = true;
    if (IoTDBDescriptor.getInstance().getConfig().getPartitionInterval() != partitionInterval) {
      partitionIntervalEquals = false;
    }
    if (ClusterConstant.HASH_SALT != hashSalt) {
      hashSaltEquals = false;
    }
    if (ClusterDescriptor.getINSTANCE().getConfig().getReplicationNum() != replicationNum) {
      replicationNumEquals = false;
    }
    CheckStatusResponse response = new CheckStatusResponse();
    response.setPartitionalIntervalEquals(partitionIntervalEquals);
    response.setHashSaltIntervalEquals(hashSaltEquals);
    response.setReplicationNumEquals(replicationNumEquals);
    resultHandler.onComplete(response);
  }
}
