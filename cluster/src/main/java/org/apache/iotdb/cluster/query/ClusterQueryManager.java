/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterQueryManager {

  private static final Logger logger = LoggerFactory.getLogger(ClusterQueryManager.class);

  private AtomicLong readerId = new AtomicLong();
  private Map<Node, Map<Long, RemoteQueryContext>> queryContextMap = new ConcurrentHashMap<>();
  private Map<Long, IPointReader> seriesReaderMap = new ConcurrentHashMap<>();
  private Map<Node, Set<Long>> nodeReaderIds = new ConcurrentHashMap<>();

  public synchronized RemoteQueryContext getQueryContext(Node node, long queryId) {
    Map<Long, RemoteQueryContext> nodeContextMap = queryContextMap.computeIfAbsent(node,
        n -> new HashMap<>());
    RemoteQueryContext remoteQueryContext = nodeContextMap.get(queryId);
    if (remoteQueryContext == null) {
      remoteQueryContext = new RemoteQueryContext(queryId, node);
      nodeContextMap.put(queryId, remoteQueryContext);
    }
    return remoteQueryContext;
  }

  public long registerReader(IPointReader pointReader) {
    long newReaderId = readerId.incrementAndGet();
    seriesReaderMap.put(newReaderId, pointReader);
    return newReaderId;
  }

  public void releaseReader(long readerId) {
    IPointReader reader = seriesReaderMap.remove(readerId);
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        logger.error("Cannot release reader {} of Id {}", reader, readerId, e);
      }
    }
  }

  public IPointReader getReader(long readerId) {
    return seriesReaderMap.get(readerId);
  }
}
