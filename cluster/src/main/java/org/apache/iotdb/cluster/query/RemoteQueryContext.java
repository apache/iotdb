/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.query.context.QueryContext;

public class RemoteQueryContext extends QueryContext {
  private Node requester;
  /**
   * The readers requested from remote nodes to serve a local query.
   */
  private Map<Node, Set<Long>> remoteReaderIdMap = new HashMap<>();
  private long remoteQueryId;
  /**
   * The readers constructed locally to respond a remote query.
   */
  private Set<Long> localReaderIds = new HashSet<>();

  public RemoteQueryContext(long jobId, Node requester) {
    super(jobId);
    this.requester = requester;
  }

  public void registerRemoteReader(Node node, long readerId) {
    remoteReaderIdMap.computeIfAbsent(node, n -> new HashSet<>()).add(readerId);
  }

  public void registerLocalReader(long readerId) {
    localReaderIds.add(readerId);
  }

  public long getRemoteQueryId() {
    return remoteQueryId;
  }

  void setRemoteQueryId(long remoteQueryId) {
    this.remoteQueryId = remoteQueryId;
  }

  public Set<Long> getLocalReaderIds() {
    return localReaderIds;
  }
}
