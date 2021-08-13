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

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.client.sync.SyncClientFactory;
import org.apache.iotdb.cluster.client.sync.SyncClientPool;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.Client;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.concurrent.atomic.AtomicInteger;

public class TestSyncClientFactory implements SyncClientFactory {
  private AtomicInteger clientSerialNum = new AtomicInteger();
  private TProtocolFactory protocolFactory = new Factory();

  public TestSyncClientFactory() {}

  @Override
  public Client getSyncClient(Node node, SyncClientPool pool) {
    TTransport dummyTransport =
        new TTransport() {
          boolean closed = false;

          @Override
          public boolean isOpen() {
            return !closed;
          }

          @Override
          public void open() {
            closed = false;
          }

          @Override
          public void close() {
            closed = true;
          }

          @Override
          public int read(byte[] bytes, int i, int i1) {
            return 0;
          }

          @Override
          public void write(byte[] bytes, int i, int i1) {
            // do nothing
          }

          @Override
          public TConfiguration getConfiguration() {
            return null;
          }

          @Override
          public void updateKnownMessageSize(long size) throws TTransportException {}

          @Override
          public void checkReadBytesAvailable(long numBytes) throws TTransportException {}
        };
    return new TestSyncClient(
        protocolFactory.getProtocol(dummyTransport),
        protocolFactory.getProtocol(dummyTransport),
        clientSerialNum.getAndIncrement());
  }
}
