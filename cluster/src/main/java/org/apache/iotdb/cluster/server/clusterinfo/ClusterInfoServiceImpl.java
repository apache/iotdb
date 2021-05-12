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

package org.apache.iotdb.cluster.server.clusterinfo;

import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.rpc.thrift.ClusterInfoService;
import org.apache.iotdb.cluster.rpc.thrift.DataPartitionEntry;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.nodetool.ClusterMonitor;

import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClusterInfoServiceImpl implements ClusterInfoService.Iface {

  @Override
  public List<Node> getRing() throws TException {
    return ClusterMonitor.INSTANCE.getRing();
  }

  @Override
  public List<DataPartitionEntry> getDataPartition(String path, long startTime, long endTime) {
    MultiKeyMap<Long, PartitionGroup> partitions =
        ClusterMonitor.INSTANCE.getDataPartition(path, startTime, endTime);
    List<DataPartitionEntry> result = new ArrayList<>(partitions.size());
    partitions.forEach(
        (multikey, nodes) ->
            result.add(new DataPartitionEntry(multikey.getKey(0), multikey.getKey(1), nodes)));
    return result;
  }

  @Override
  public List<Node> getMetaPartition(String path) throws TException {
    return ClusterMonitor.INSTANCE.getMetaPartition(path);
  }

  @Override
  public Map<Node, Boolean> getAllNodeStatus() throws TException {
    return ClusterMonitor.INSTANCE.getAllNodeStatus();
  }

  @Override
  public String getInstrumentingInfo() throws TException {
    return ClusterMonitor.INSTANCE.getInstrumentingInfo();
  }

  public void handleClientExit() {
    // do something when a client connection exits.
  }
}
