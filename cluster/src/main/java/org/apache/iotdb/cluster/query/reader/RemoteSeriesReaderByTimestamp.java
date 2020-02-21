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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.handlers.caller.GenericHandler;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteSeriesReaderByTimestamp implements IReaderByTimestamp {

  private static final Logger logger = LoggerFactory.getLogger(RemoteSimpleSeriesReader.class);
  private long readerId;
  private Node source;
  private Node header;
  private MetaGroupMember metaGroupMember;

  private AtomicReference<ByteBuffer> fetchResult = new AtomicReference<>();
  private GenericHandler<ByteBuffer> handler;

  public RemoteSeriesReaderByTimestamp(long readerId, Node source,
      Node header, MetaGroupMember metaGroupMember) {
    this.readerId = readerId;
    this.source = source;
    this.header = header;
    this.metaGroupMember = metaGroupMember;
    handler = new GenericHandler<>(source, fetchResult);
  }

  @Override
  public Object getValueInTimestamp(long timestamp) throws IOException {
    DataClient client = metaGroupMember.getDataClient(source);
    synchronized (fetchResult) {
      fetchResult.set(null);
      try {
        client.fetchSingleSeriesByTimestamp(header, readerId, timestamp, handler);
        fetchResult.wait(connectionTimeoutInMS);
      } catch (TException | InterruptedException e) {
        throw new IOException(e);
      }
    }
    return SerializeUtils.deserializeObject(fetchResult.get());
  }

  @Override
  public boolean hasNext() {
    return true;
  }
}
