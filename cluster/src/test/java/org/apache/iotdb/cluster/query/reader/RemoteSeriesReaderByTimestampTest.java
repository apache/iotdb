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

import org.apache.iotdb.cluster.client.DataClientProvider;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftNode;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RemoteSeriesReaderByTimestampTest {

  private BatchData batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);
  private Set<Node> failedNodes = new ConcurrentSkipListSet<>();
  private boolean prevUseAsyncServer;

  @Before
  public void setUp() {
    prevUseAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    metaGroupMember.setClientProvider(
        new DataClientProvider(new Factory()) {
          @Override
          public AsyncDataClient getAsyncDataClient(Node node, int timeout) throws IOException {
            return new AsyncDataClient(null, null, node, null) {
              @Override
              public void fetchSingleSeriesByTimestamps(
                  RaftNode header,
                  long readerId,
                  List<Long> timestamps,
                  AsyncMethodCallback<ByteBuffer> resultHandler)
                  throws TException {
                if (failedNodes.contains(node)) {
                  throw new TException("Node down.");
                }

                new Thread(
                        () -> {
                          ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                          DataOutputStream dataOutputStream =
                              new DataOutputStream(byteArrayOutputStream);
                          Object[] results = new Object[timestamps.size()];
                          for (int i = 0; i < timestamps.size(); i++) {
                            while (batchData.hasCurrent()) {
                              long currentTime = batchData.currentTime();
                              if (currentTime == timestamps.get(i)) {
                                results[i] = batchData.currentValue();
                                batchData.next();
                                break;
                              } else if (currentTime > timestamps.get(i)) {
                                results[i] = null;
                                break;
                              }
                              // time < timestamp, continue
                              batchData.next();
                            }
                          }
                          SerializeUtils.serializeObjects(results, dataOutputStream);

                          resultHandler.onComplete(
                              ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
                        })
                    .start();
              }

              @Override
              public void querySingleSeriesByTimestamp(
                  SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler)
                  throws TException {
                if (failedNodes.contains(node)) {
                  throw new TException("Node down.");
                }

                new Thread(() -> resultHandler.onComplete(1L)).start();
              }
            };
          }
        });
  }

  @After
  public void tearDown() {
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(prevUseAsyncServer);
  }

  private MetaGroupMember metaGroupMember = new MetaGroupMember();

  @Test
  public void test() throws IOException, StorageEngineException {
    PartitionGroup group = new PartitionGroup();
    group.add(TestUtils.getNode(0));
    group.add(TestUtils.getNode(1));
    group.add(TestUtils.getNode(2));

    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    RemoteQueryContext context = new RemoteQueryContext(1);

    try {
      DataSourceInfo sourceInfo =
          new DataSourceInfo(group, TSDataType.DOUBLE, request, context, metaGroupMember, group);
      sourceInfo.hasNextDataClient(true, Long.MIN_VALUE);

      RemoteSeriesReaderByTimestamp reader = new RemoteSeriesReaderByTimestamp(sourceInfo);

      long[] times = new long[100];
      for (int i = 0; i < 100; i++) {
        times[i] = i;
      }
      Object[] results = reader.getValuesInTimestamps(times, times.length);
      for (int i = 0; i < 100; i++) {
        assertEquals(i * 1.0, results[i]);
      }
      times[0] = 101;
      assertEquals(null, reader.getValuesInTimestamps(times, 1)[0]);
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testFailedNode() throws IOException, StorageEngineException {
    batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);
    PartitionGroup group = new PartitionGroup();
    group.add(TestUtils.getNode(0));
    group.add(TestUtils.getNode(1));
    group.add(TestUtils.getNode(2));

    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    RemoteQueryContext context = new RemoteQueryContext(1);

    try {
      DataSourceInfo sourceInfo =
          new DataSourceInfo(group, TSDataType.DOUBLE, request, context, metaGroupMember, group);
      long startTime = System.currentTimeMillis();
      sourceInfo.hasNextDataClient(true, Long.MIN_VALUE);
      RemoteSeriesReaderByTimestamp reader = new RemoteSeriesReaderByTimestamp(sourceInfo);

      long endTime = System.currentTimeMillis();
      System.out.println(
          Thread.currentThread().getStackTrace()[1].getLineNumber()
              + " begin: "
              + (endTime - startTime));
      // normal read
      assertEquals(TestUtils.getNode(0), sourceInfo.getCurrentNode());
      long[] times = new long[50];
      for (int i = 0; i < 50; i++) {
        times[i] = i;
      }
      Object[] results = reader.getValuesInTimestamps(times, 50);
      for (int i = 0; i < 50; i++) {
        assertEquals(i * 1.0, results[i]);
      }

      endTime = System.currentTimeMillis();
      System.out.println(
          Thread.currentThread().getStackTrace()[1].getLineNumber()
              + " begin: "
              + (endTime - startTime));
      failedNodes.add(TestUtils.getNode(0));
      for (int i = 50; i < 80; i++) {
        times[i - 50] = i;
      }
      results = reader.getValuesInTimestamps(times, 30);
      for (int i = 50; i < 80; i++) {
        assertEquals(i * 1.0, results[i - 50]);
      }
      assertEquals(TestUtils.getNode(1), sourceInfo.getCurrentNode());

      // a bad client, change to another node again
      failedNodes.add(TestUtils.getNode(1));
      for (int i = 80; i < 90; i++) {
        times[i - 80] = i;
      }
      results = reader.getValuesInTimestamps(times, 10);
      for (int i = 80; i < 90; i++) {
        assertEquals(i * 1.0, results[i - 80]);
      }
      assertEquals(TestUtils.getNode(2), sourceInfo.getCurrentNode());

      endTime = System.currentTimeMillis();
      System.out.println(
          Thread.currentThread().getStackTrace()[1].getLineNumber()
              + " begin: "
              + (endTime - startTime));
      // all node failed
      failedNodes.add(TestUtils.getNode(2));

      try {
        times[0] = 90;
        reader.getValuesInTimestamps(times, 1);
        fail();
      } catch (IOException e) {
        assertEquals("no available client.", e.getMessage());
      }
      endTime = System.currentTimeMillis();
      System.out.println(
          Thread.currentThread().getStackTrace()[1].getLineNumber()
              + " begin: "
              + (endTime - startTime));
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }
}
