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

package org.apache.iotdb.cluster.server.handlers.caller;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.nodetool.function.Status;

import org.apache.thrift.async.AsyncMethodCallback;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeStatusHandler implements AsyncMethodCallback<Node> {

  private Map<Node, Integer> nodeStatusMap;

  private AtomicInteger countResponse;

  public NodeStatusHandler(Map<Node, Integer> nodeStatusMap) {
    this.nodeStatusMap = nodeStatusMap;
    this.countResponse = new AtomicInteger();
  }

  @Override
  public void onComplete(Node response) {
    synchronized (nodeStatusMap) {
      if (response == null) {
        return;
      }
      nodeStatusMap.put(response, Status.LIVE);
      // except for this node itself
      if (countResponse.incrementAndGet() == nodeStatusMap.size() - 1) {
        nodeStatusMap.notifyAll();
      }
    }
  }

  @Override
  public void onError(Exception exception) {
    // unused
  }
}
