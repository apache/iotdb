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
package org.apache.iotdb.cluster.server;

import org.apache.iotdb.cluster.rpc.thrift.ClusterInfoService;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.nodetool.ClusterMonitor;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

public class ClusterInfoClientServer implements ClusterInfoService.Iface {

  @Override
  public List<Node> getRing() throws TException {
    return ClusterMonitor.INSTANCE.getRing();
  }

  @Override
  public Map<Long, List<Node>> getDataPartition(String path, long startTime, long endTime)
      throws TException {
    //    Map<Long, PartitionGroup> result =
    //        ClusterMonitor.INSTANCE.getDataPartition(path, startTime, endTime);

    return null;
  }

  @Override
  public List<Node> getMetaPartition(String path) throws TException {
    return null;
  }

  @Override
  public Map<Node, Boolean> getAllNodeStatus() throws TException {
    return null;
  }

  @Override
  public String getInstrumentingInfo() throws TException {
    return null;
  }

  @Override
  public void resetInstrumenting() throws TException {}
}
