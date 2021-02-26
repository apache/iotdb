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
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

public class RemoteSimpleSeriesReaderTest {

  private RemoteSimpleSeriesReader reader;
  private BatchData batchData;
  private boolean batchUsed;
  private MetaGroupMember metaGroupMember;
  private Set<Node> failedNodes = new ConcurrentSkipListSet<>();
  private boolean prevUseAsyncServer;

  @Before
  public void setUp() {
    prevUseAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);
    batchUsed = false;
    metaGroupMember = new TestMetaGroupMember();
    metaGroupMember.setClientProvider(
        new DataClientProvider(new Factory()) {
          @Override
          public AsyncDataClient getAsyncDataClient(Node node, int timeout) throws IOException {
            return new AsyncDataClient(null, null, node, null) {
              @Override
              public void fetchSingleSeries(
                  Node header, long readerId, AsyncMethodCallback<ByteBuffer> resultHandler)
                  throws TException {
                if (failedNodes.contains(node)) {
                  throw new TException("Node down.");
                }

                new Thread(
                        () -> {
                          if (batchUsed) {
                            resultHandler.onComplete(ByteBuffer.allocate(0));
                          } else {
                            ByteArrayOutputStream byteArrayOutputStream =
                                new ByteArrayOutputStream();
                            DataOutputStream dataOutputStream =
                                new DataOutputStream(byteArrayOutputStream);
                            SerializeUtils.serializeBatchData(batchData, dataOutputStream);
                            batchUsed = true;
                            resultHandler.onComplete(
                                ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
                          }
                        })
                    .start();
              }

              @Override
              public void querySingleSeries(
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

  @Test
  public void testSingle() throws IOException, StorageEngineException {
    PartitionGroup group = new PartitionGroup();
    group.add(TestUtils.getNode(0));
    group.add(TestUtils.getNode(1));
    group.add(TestUtils.getNode(2));

    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    RemoteQueryContext context = new RemoteQueryContext(1);

    try {
      DataSourceInfo sourceInfo =
          new DataSourceInfo(group, TSDataType.DOUBLE, request, context, metaGroupMember, group);
      sourceInfo.hasNextDataClient(false, Long.MIN_VALUE);

      reader = new RemoteSimpleSeriesReader(sourceInfo);

      for (int i = 0; i < 100; i++) {
        assertTrue(reader.hasNextTimeValuePair());
        TimeValuePair curr = reader.currentTimeValuePair();
        TimeValuePair pair = reader.nextTimeValuePair();
        assertEquals(pair, curr);
        assertEquals(i, pair.getTimestamp());
        assertEquals(i * 1.0, pair.getValue().getDouble(), 0.00001);
      }
      assertFalse(reader.hasNextTimeValuePair());
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testFailedNode() throws IOException, StorageEngineException {
    System.out.println("Start testFailedNode()");

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
      sourceInfo.hasNextDataClient(false, Long.MIN_VALUE);
      reader = new RemoteSimpleSeriesReader(sourceInfo);

      // normal read
      Assert.assertEquals(TestUtils.getNode(0), sourceInfo.getCurrentNode());
      for (int i = 0; i < 50; i++) {
        assertTrue(reader.hasNextTimeValuePair());
        TimeValuePair curr = reader.currentTimeValuePair();
        TimeValuePair pair = reader.nextTimeValuePair();
        assertEquals(pair, curr);
        assertEquals(i, pair.getTimestamp());
        assertEquals(i * 1.0, pair.getValue().getDouble(), 0.00001);
      }

      this.batchUsed = false;
      this.batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);
      // a bad client, change to another node
      failedNodes.add(TestUtils.getNode(0));
      reader.clearCurDataForTest();
      for (int i = 50; i < 80; i++) {
        TimeValuePair pair = reader.nextTimeValuePair();
        assertEquals(i - 50, pair.getTimestamp());
        assertEquals((i - 50) * 1.0, pair.getValue().getDouble(), 0.00001);
      }
      Assert.assertEquals(TestUtils.getNode(1), sourceInfo.getCurrentNode());

      this.batchUsed = false;
      this.batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);
      // a bad client, change to another node again
      failedNodes.add(TestUtils.getNode(1));
      reader.clearCurDataForTest();
      for (int i = 80; i < 90; i++) {
        TimeValuePair pair = reader.nextTimeValuePair();
        assertEquals(i - 80, pair.getTimestamp());
        assertEquals((i - 80) * 1.0, pair.getValue().getDouble(), 0.00001);
      }
      assertEquals(TestUtils.getNode(2), sourceInfo.getCurrentNode());

      // all node failed
      failedNodes.add(TestUtils.getNode(2));
      reader.clearCurDataForTest();
      try {
        reader.nextTimeValuePair();
        fail();
      } catch (IOException e) {
        assertEquals(e.getMessage(), "no available client.");
      }
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }
}
