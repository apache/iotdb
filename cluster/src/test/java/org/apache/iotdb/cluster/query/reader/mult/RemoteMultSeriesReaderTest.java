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
package org.apache.iotdb.cluster.query.reader.mult;

import org.apache.iotdb.cluster.client.DataClientProvider;
import org.apache.iotdb.cluster.client.async.AsyncDataClient;
import org.apache.iotdb.cluster.client.sync.SyncDataClient;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.MultSeriesQueryRequest;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class RemoteMultSeriesReaderTest {

  private RemoteMultSeriesReader reader;
  private List<BatchData> batchData;
  private boolean batchUsed;
  private MetaGroupMember metaGroupMember;
  private Set<Node> failedNodes = new ConcurrentSkipListSet<>();
  private boolean prevUseAsyncServer;
  private List<PartialPath> paths;
  private List<TSDataType> dataTypes;

  @Before
  public void setUp() throws IllegalPathException {
    paths = Lists.newArrayList();
    dataTypes = Lists.newArrayList();
    PartialPath partialPath = new PartialPath("root.a.b");
    paths.add(partialPath);
    partialPath = new PartialPath("root.a.c");
    paths.add(partialPath);
    dataTypes.add(TSDataType.DOUBLE);
    dataTypes.add(TSDataType.INT32);
    prevUseAsyncServer = ClusterDescriptor.getInstance().getConfig().isUseAsyncServer();
    batchData = Lists.newArrayList();
    batchData.add(TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100));
    batchData.add(TestUtils.genBatchData(TSDataType.INT32, 0, 100));
    batchUsed = false;
    metaGroupMember = new TestMetaGroupMember();
  }

  @After
  public void tearDown() {
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(prevUseAsyncServer);
  }

  @Test
  public void testAsyncMultSeriesReader() throws IOException, StorageEngineException {
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(true);
    PartitionGroup group = new PartitionGroup();
    setAsyncDataClient();
    group.add(TestUtils.getNode(0));
    group.add(TestUtils.getNode(1));
    group.add(TestUtils.getNode(2));

    MultSeriesQueryRequest request = new MultSeriesQueryRequest();
    RemoteQueryContext context = new RemoteQueryContext(1);

    try {
      MultDataSourceInfo sourceInfo =
          new MultDataSourceInfo(group, paths, dataTypes, request, context, metaGroupMember, group);
      sourceInfo.hasNextDataClient(Long.MIN_VALUE);

      reader = new RemoteMultSeriesReader(sourceInfo);

      for (int i = 0; i < 100; i++) {
        assertTrue(reader.hasNextTimeValuePair(paths.get(0).getFullPath()));
        TimeValuePair pair = reader.nextTimeValuePair(paths.get(0).getFullPath());
        assertEquals(i, pair.getTimestamp());
        assertEquals(i * 1.0, pair.getValue().getDouble(), 0.00001);
      }
      assertFalse(reader.hasNextTimeValuePair(paths.get(0).getFullPath()));

    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testSyncMultSeriesReader() throws IOException, StorageEngineException {
    ClusterDescriptor.getInstance().getConfig().setUseAsyncServer(false);
    setSyncDataClient();
    PartitionGroup group = new PartitionGroup();
    group.add(TestUtils.getNode(0));
    group.add(TestUtils.getNode(1));
    group.add(TestUtils.getNode(2));

    MultSeriesQueryRequest request = new MultSeriesQueryRequest();
    RemoteQueryContext context = new RemoteQueryContext(1);

    try {
      MultDataSourceInfo sourceInfo =
          new MultDataSourceInfo(group, paths, dataTypes, request, context, metaGroupMember, group);
      sourceInfo.hasNextDataClient(Long.MIN_VALUE);

      reader = new RemoteMultSeriesReader(sourceInfo);

      for (int i = 0; i < 100; i++) {
        assertTrue(reader.hasNextTimeValuePair(paths.get(0).getFullPath()));
        TimeValuePair pair = reader.nextTimeValuePair(paths.get(0).getFullPath());
        assertEquals(i, pair.getTimestamp());
        assertEquals(i * 1.0, pair.getValue().getDouble(), 0.00001);
      }
      assertFalse(reader.hasNextTimeValuePair(paths.get(0).getFullPath()));

      for (int i = 0; i < 100; i++) {
        assertTrue(reader.hasNextTimeValuePair(paths.get(1).getFullPath()));
        TimeValuePair pair = reader.nextTimeValuePair(paths.get(1).getFullPath());
        assertEquals(i, pair.getTimestamp());
        assertEquals(i * 1.0, pair.getValue().getInt(), 0.00001);
      }
      assertFalse(reader.hasNextTimeValuePair(paths.get(1).getFullPath()));
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }

  @Test
  public void testDefaultBatchStrategySelect() {
    RemoteMultSeriesReader.DefaultBatchStrategy defaultBatchStrategy =
        new RemoteMultSeriesReader.DefaultBatchStrategy();
    Map cachedBatches = Maps.newHashMap();
    Queue queue = new ConcurrentLinkedQueue<BatchData>();
    batchData.forEach(
        data -> {
          queue.add(data);
        });
    cachedBatches.put("root.a.b", queue);
    assertEquals(1, defaultBatchStrategy.selectBatchPaths(cachedBatches).size());
  }

  private void setAsyncDataClient() {
    metaGroupMember.setClientProvider(
        new DataClientProvider(new Factory()) {
          @Override
          public AsyncDataClient getAsyncDataClient(Node node, int timeout) throws IOException {
            return new AsyncDataClient(null, null, node, null) {
              @Override
              public void fetchMultSeries(
                  Node header,
                  long readerId,
                  List<String> paths,
                  AsyncMethodCallback<Map<String, ByteBuffer>> resultHandler)
                  throws TException {
                if (failedNodes.contains(node)) {
                  throw new TException("Node down.");
                }

                new Thread(
                        () -> {
                          Map<String, ByteBuffer> stringByteBufferMap = Maps.newHashMap();
                          if (batchUsed) {
                            paths.forEach(
                                path -> {
                                  stringByteBufferMap.put(path, ByteBuffer.allocate(0));
                                });
                          } else {
                            batchUsed = true;

                            for (int i = 0; i < batchData.size(); i++) {
                              stringByteBufferMap.put(
                                  paths.get(i), generateByteBuffer(batchData.get(i)));
                            }

                            resultHandler.onComplete(stringByteBufferMap);
                          }
                        })
                    .start();
              }

              @Override
              public void queryMultSeries(
                  MultSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler)
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

  private void setSyncDataClient() {
    metaGroupMember.setClientProvider(
        new DataClientProvider(new Factory()) {
          @Override
          public SyncDataClient getSyncDataClient(Node node, int timeout) {
            return new SyncDataClient(null) {
              @Override
              public Map<String, ByteBuffer> fetchMultSeries(
                  Node header, long readerId, List<String> paths) throws TException {
                if (failedNodes.contains(node)) {
                  throw new TException("Node down.");
                }

                Map<String, ByteBuffer> stringByteBufferMap = Maps.newHashMap();
                if (batchUsed) {
                  paths.forEach(
                      path -> {
                        stringByteBufferMap.put(path, ByteBuffer.allocate(0));
                      });
                } else {
                  batchUsed = true;
                  for (int i = 0; i < batchData.size(); i++) {
                    stringByteBufferMap.put(paths.get(i), generateByteBuffer(batchData.get(i)));
                  }
                }
                return stringByteBufferMap;
              }

              @Override
              public long queryMultSeries(MultSeriesQueryRequest request) throws TException {
                if (failedNodes.contains(node)) {
                  throw new TException("Node down.");
                }

                return 1L;
              }
            };
          }
        });
  }

  private ByteBuffer generateByteBuffer(BatchData batchData) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    SerializeUtils.serializeBatchData(batchData, dataOutputStream);
    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
    return byteBuffer;
  }
}
