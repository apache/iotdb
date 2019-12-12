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
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterQueryManager {

  private static final Logger logger = LoggerFactory.getLogger(ClusterQueryManager.class);

  private AtomicLong readerIdAtom = new AtomicLong();
  private Map<Node, Map<Long, RemoteQueryContext>> queryContextMap = new ConcurrentHashMap<>();
  private Map<Long, IPointReader> seriesReaderMap = new ConcurrentHashMap<>();

  public synchronized RemoteQueryContext getQueryContext(Node node, long queryId) {
    Map<Long, RemoteQueryContext> nodeContextMap = queryContextMap.computeIfAbsent(node,
        n -> new HashMap<>());
    RemoteQueryContext remoteQueryContext = nodeContextMap.get(queryId);
    if (remoteQueryContext == null) {
      remoteQueryContext = new RemoteQueryContext(QueryResourceManager.getInstance().assignJobId(), node);
      remoteQueryContext.setRemoteQueryId(queryId);
      nodeContextMap.put(queryId, remoteQueryContext);
    }
    return remoteQueryContext;
  }

  public long registerReader(IPointReader pointReader) {
    long newReaderId = readerIdAtom.incrementAndGet();
    seriesReaderMap.put(newReaderId, pointReader);
    return newReaderId;
  }

  public synchronized void endQuery(Node node, long queryId) throws StorageEngineException {
    Map<Long, RemoteQueryContext> nodeContextMap = queryContextMap.get(node);
    if (nodeContextMap == null) {
      return;
    }
    RemoteQueryContext remoteQueryContext = nodeContextMap.remove(queryId);
    if (remoteQueryContext == null) {
      return;
    }
    // release file resources
    QueryResourceManager.getInstance().endQueryForGivenJob(remoteQueryContext.getJobId());

    // remove the readers from the cache
    Set<Long> readerIds = remoteQueryContext.getLocalReaderIds();
    for (long readerId : readerIds) {
      IPointReader pointReader = seriesReaderMap.remove(readerId);
      if (pointReader != null) {
        try {
          pointReader.close();
        } catch (IOException e) {
          logger.error("Cannot close a reader for query {} of Node {}", queryId, node, e);
        }
      }
    }
  }

  public IPointReader getReader(long readerId) {
    return seriesReaderMap.get(readerId);
  }
}
