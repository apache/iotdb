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

import static org.apache.iotdb.cluster.server.RaftServer.connectionTimeoutInMS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RemoteSimpleSeriesReader is a reader without value filter that reads points from a remote side.
 */
public class RemoteSimpleSeriesReader implements ManagedSeriesReader {

  private static final Logger logger = LoggerFactory.getLogger(RemoteSimpleSeriesReader.class);
  long readerId;
  Node source;
  Node header;
  MetaGroupMember metaGroupMember;

  private BatchData cachedBatch;
  private TimeValuePair cachedPair;

  private AtomicReference<ByteBuffer> fetchResult = new AtomicReference<>();
  private GenericHandler<ByteBuffer> handler;

  private volatile boolean managedByPool;
  private volatile boolean hasRemaining;

  public RemoteSimpleSeriesReader(long readerId, Node source,
      Node header, MetaGroupMember metaGroupMember) {
    this.readerId = readerId;
    this.source = source;
    this.header = header;
    this.metaGroupMember = metaGroupMember;
    handler = new GenericHandler<>(source, fetchResult);
  }

  @Override
  public boolean hasNextBatch() throws IOException {
    if (cachedBatch != null) {
      return true;
    }
    fetchBatch();
    return cachedBatch != null;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    if (!hasNextBatch()) {
      throw new NoSuchElementException();
    }
    BatchData ret = cachedBatch;
    cachedBatch = null;
    return ret;
  }


  @Override
  public boolean hasNext() throws IOException {
    if (cachedPair != null) {
      return true;
    }
    fetchPoint();
    return cachedPair != null;
  }

  private void fetchPoint() throws IOException {
    if ((cachedBatch == null || !cachedBatch.hasCurrent()) && hasNextBatch()) {
      cachedBatch = nextBatch();
    }
    if (cachedBatch != null && cachedBatch.hasCurrent()) {
      cachedPair = new TimeValuePair(cachedBatch.currentTime(),
          TsPrimitiveType.getByType(cachedBatch.getDataType(), cachedBatch.currentValue()));
      cachedBatch.next();
    }
  }

  @Override
  public TimeValuePair next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    TimeValuePair ret = cachedPair;
    cachedPair = null;
    return ret;
  }

  @Override
  public TimeValuePair current() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return cachedPair;
  }

  @Override
  public void close() {
    // close by Resource manager
  }

  @Override
  public boolean isManagedByQueryManager() {
    return managedByPool;
  }

  @Override
  public void setManagedByQueryManager(boolean managedByQueryManager) {
    managedByPool = managedByQueryManager;
  }

  @Override
  public boolean hasRemaining() {
    return hasRemaining;
  }

  @Override
  public void setHasRemaining(boolean hasRemaining) {
    this.hasRemaining = hasRemaining;
  }

  private void fetchBatch() throws IOException {
    DataClient client = (DataClient) metaGroupMember.getDataClientPool().getClient(source);
    synchronized (fetchResult) {
      fetchResult.set(null);
      try {
        client.fetchSingleSeries(header, readerId, handler);
        fetchResult.wait(connectionTimeoutInMS);
      } catch (TException | InterruptedException e) {
        throw new IOException(e);
      }
    }
    cachedBatch = SerializeUtils.deserializeBatchData(fetchResult.get());
    if (logger.isDebugEnabled()) {
      logger.debug("Fetched a batch from {}, size:{}", source,
          cachedBatch == null ? 0 : cachedBatch.length());
    }
  }
}
