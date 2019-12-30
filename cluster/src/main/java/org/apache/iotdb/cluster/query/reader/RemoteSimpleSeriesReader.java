/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query.reader;

import static org.apache.iotdb.cluster.server.RaftServer.CONNECTION_TIME_OUT_MS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RemoteSimpleSeriesReader is a reader without value filter that reads points from a remote side.
 */
public class RemoteSimpleSeriesReader implements IBatchReader {

  private static final Logger logger = LoggerFactory.getLogger(RemoteSimpleSeriesReader.class);
  private long readerId;
  private Node source;
  private Node header;
  private MetaGroupMember metaGroupMember;

  private BatchData cache;

  private AtomicReference<ByteBuffer> fetchResult = new AtomicReference<>();
  private GenericHandler<ByteBuffer> handler;

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
    if (cache != null) {
      return true;
    }
    fetch();
    return cache != null;
  }

  @Override
  public BatchData nextBatch() throws IOException {
    if (!hasNextBatch()) {
      throw new NoSuchElementException();
    }
    BatchData ret = cache;
    cache = null;
    return ret;
  }


  @Override
  public void close() {
    // close by Resource manager
  }

  private void fetch() throws IOException {
    DataClient client = (DataClient) metaGroupMember.getDataClientPool().getClient(source);
    synchronized (fetchResult) {
      fetchResult.set(null);
      try {
        client.fetchSingleSeries(header, readerId, handler);
        fetchResult.wait(CONNECTION_TIME_OUT_MS);
      } catch (TException | InterruptedException e) {
        throw new IOException(e);
      }
    }
    cache = SerializeUtils.deserializeBatchData(fetchResult.get());
    logger.debug("Fetched a batch from {}, size:{}", source, cache.length());
  }
}
