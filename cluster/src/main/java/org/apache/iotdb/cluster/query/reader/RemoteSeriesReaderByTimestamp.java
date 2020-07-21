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

package org.apache.iotdb.cluster.query.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.server.RaftServer;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteSeriesReaderByTimestamp implements IReaderByTimestamp {

  private static final Logger logger = LoggerFactory.getLogger(RemoteSeriesReaderByTimestamp.class);
  private DataSourceInfo sourceInfo;

  private AtomicReference<ByteBuffer> fetchResult = new AtomicReference<>();
  private GenericHandler<ByteBuffer> handler;

  public RemoteSeriesReaderByTimestamp(DataSourceInfo sourceInfo) {
    this.sourceInfo = sourceInfo;
    handler = new GenericHandler<>(sourceInfo.getCurrentNode(), fetchResult);
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    if (!sourceInfo.checkCurClient()) {
      return null;
    }

    ByteBuffer result;
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      result = fetchResultAsync(timestamp);
    } else {
      result = fetchResultSync(timestamp);
    }

    return SerializeUtils.deserializeObject(result);
  }

  @SuppressWarnings("java:S2274") // enable timeout
  private ByteBuffer fetchResultAsync(long timestamp) throws IOException {
    synchronized (fetchResult) {
      fetchResult.set(null);
      try {
        sourceInfo.getCurAsyncClient(RaftServer.getReadOperationTimeoutMS())
            .fetchSingleSeriesByTimestamp(sourceInfo.getHeader(),
                sourceInfo.getReaderId(), timestamp, handler);
        fetchResult.wait(RaftServer.getReadOperationTimeoutMS());
      } catch (TException e) {
        //try other node
        if (!sourceInfo.switchNode(true, timestamp)) {
          return null;
        }
        return fetchResultAsync(timestamp);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Query {} interrupted", sourceInfo);
        return null;
      }
    }
    return fetchResult.get();
  }

  private ByteBuffer fetchResultSync(long timestamp) throws IOException {
    try {
      SyncDataClient curSyncClient = sourceInfo
          .getCurSyncClient(RaftServer.getReadOperationTimeoutMS());
      ByteBuffer buffer = curSyncClient
          .fetchSingleSeriesByTimestamp(sourceInfo.getHeader(),
              sourceInfo.getReaderId(), timestamp);
      curSyncClient.putBack();
      return buffer;
    } catch (TException e) {
      //try other node
      if (!sourceInfo.switchNode(true, timestamp)) {
        return null;
      }
      return fetchResultSync(timestamp);
    }
  }
}
