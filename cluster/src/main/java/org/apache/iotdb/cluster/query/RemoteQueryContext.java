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

package org.apache.iotdb.cluster.query;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.db.query.context.QueryContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

public class RemoteQueryContext extends QueryContext {
  /** The remote nodes that are queried in this query, grouped by the header nodes. */
  private Map<RaftNode, Set<Node>> queriedNodesMap = new HashMap<>();
  /** The readers constructed locally to respond a remote query. */
  private Set<Long> localReaderIds = new ConcurrentSkipListSet<>();

  /** The readers constructed locally to respond a remote query. */
  private Set<Long> localGroupByExecutorIds = new ConcurrentSkipListSet<>();

  public RemoteQueryContext(long jobId) {
    super(jobId);
  }

  public RemoteQueryContext(
      long jobId, boolean debug, long startTime, String statement, long timeout) {
    super(jobId, debug, startTime, statement, timeout);
  }

  public void registerRemoteNode(Node node, RaftNode header) {
    queriedNodesMap.computeIfAbsent(header, n -> new HashSet<>()).add(node);
  }

  public void registerLocalReader(long readerId) {
    localReaderIds.add(readerId);
  }

  public void registerLocalGroupByExecutor(long executorId) {
    localGroupByExecutorIds.add(executorId);
  }

  public Set<Long> getLocalReaderIds() {
    return localReaderIds;
  }

  public Set<Long> getLocalGroupByExecutorIds() {
    return localGroupByExecutorIds;
  }

  public Map<RaftNode, Set<Node>> getQueriedNodesMap() {
    return queriedNodesMap;
  }
}
