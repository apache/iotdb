/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.query;

import static org.apache.iotdb.cluster.server.RaftServer.CONNECTION_TIME_OUT_MS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.thrift.TException;

/**
 * RemoteSimpleSeriesReader is a reader without value filter that reads points from a remote side.
 */
public class RemoteSimpleSeriesReader implements IPointReader {

  private long readerId;
  private Node source;
  private Node header;
  private MetaGroupMember metaGroupMember;

  private int fetchSize = 1000;
  private List<TimeValuePair> cache = Collections.emptyList();
  private int cacheIndex = 0;

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
  public boolean hasNext() throws IOException {
    if (cacheIndex < cache.size()) {
      return true;
    }
    fetch();
    return cacheIndex < cache.size();
  }

  @Override
  public TimeValuePair next() throws IOException {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return cache.get(cacheIndex++);
  }

  @Override
  public TimeValuePair current() throws IOException {
    hasNext();
    return cache.get(cacheIndex);
  }

  @Override
  public void close() throws IOException {
    // close by Resource manager
  }

  private void fetch() throws IOException {
    DataClient client = (DataClient) metaGroupMember.getDataClientPool().getClient(source);
    synchronized (fetchResult) {
      fetchResult.set(null);
      try {
        client.fetchSingleSeries(header, readerId, fetchSize, handler);
        fetchResult.wait(CONNECTION_TIME_OUT_MS);
      } catch (TException | InterruptedException e) {
        throw new IOException(e);
      }
    }
    cache = SerializeUtils.deserializeTVPairs(fetchResult.get());
    cacheIndex = 0;
  }

}
