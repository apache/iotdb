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

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RemoteSimpleSeriesReaderTest {

  private RemoteSimpleSeriesReader reader;
  private BatchData batchData;
  private boolean batchUsed;
  private MetaGroupMember metaGroupMember;

  @Before
  public void setUp() throws IOException {
    batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);
    batchUsed = false;
    metaGroupMember = new TestMetaGroupMember() {

      @Override
      public DataClient getDataClient(Node node) throws IOException {
        return new DataClient(null, null, node, null){
          @Override
          public void fetchSingleSeries(Node header, long readerId,
                                        AsyncMethodCallback<ByteBuffer> resultHandler) {
            new Thread(() -> {
              if (batchUsed) {
                resultHandler.onComplete(ByteBuffer.allocate(0));
              } else {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
                SerializeUtils.serializeBatchData(batchData, dataOutputStream);
                batchUsed = true;
                resultHandler.onComplete(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
              }
            }).start();
          }

          @Override
          public void querySingleSeries(SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) {
            new Thread(() -> {
              resultHandler.onComplete(1L);
            }).start();
          }
        };
      }
    };
  }


  @Test
  public void testSingle() throws IOException {
    PartitionGroup group = new PartitionGroup();
    group.add(TestUtils.getNode(0));
    group.add(TestUtils.getNode(1));
    group.add(TestUtils.getNode(2));

    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    RemoteQueryContext context = new RemoteQueryContext(1);

    DataSourceInfo sourceInfo = new DataSourceInfo(group, TSDataType.DOUBLE,
      request, context, metaGroupMember, group);
    sourceInfo.nextDataClient(false, Long.MIN_VALUE);

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
  }

  @Test
  public void testFailedNode() throws IOException {
    System.out.println("Start testFailedNode()");

    batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);
    PartitionGroup group = new PartitionGroup();
    group.add(TestUtils.getNode(0));
    group.add(TestUtils.getNode(1));
    group.add(TestUtils.getNode(2));

    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    RemoteQueryContext context = new RemoteQueryContext(1);

    DataSourceInfo sourceInfo = new DataSourceInfo(group, TSDataType.DOUBLE,
      request, context, metaGroupMember, group);
    sourceInfo.nextDataClient(false, Long.MIN_VALUE);
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

    // a bad client, change to another node
    DataClient badClient = new DataClient(null, null, TestUtils.getNode(0), null){
      @Override
      public void fetchSingleSeries(Node header, long readerId,
                                    AsyncMethodCallback<ByteBuffer> resultHandler)  throws TException {
        throw new TException("Good bye");
      }

      @Override
      public void querySingleSeries(SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) {
        new Thread(() -> {
          resultHandler.onComplete(1L);
        }).start();
      }
    };

    this.batchUsed = false;
    this.batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);
    // a bad client, change to another node
    reader.setClientForTest(badClient);
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
    reader.setClientForTest(badClient);
    reader.clearCurDataForTest();
    for (int i = 80; i < 90; i++) {
      TimeValuePair pair = reader.nextTimeValuePair();
      assertEquals(i - 80, pair.getTimestamp());
      assertEquals((i - 80) * 1.0, pair.getValue().getDouble(), 0.00001);
    }
    assertEquals(TestUtils.getNode(2), sourceInfo.getCurrentNode());

    // all node failed
    reader.setClientForTest(badClient);
    reader.clearCurDataForTest();
    {
      MetaGroupMember tmpMetaGroupMember = new MetaGroupMember() {
        @Override
        public DataClient getDataClient(Node node) throws IOException {
          return new DataClient(null, null, node, null) {
            @Override
            public void querySingleSeries(SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler) throws TException {
              throw new TException("Don't worry, this is the exception I constructed.");
            }
          };
        }
      };
      sourceInfo.setMetaGroupMemberForTest(tmpMetaGroupMember);
    }
    try {
      reader.nextTimeValuePair();
      fail();
    } catch (IOException e) {
      assertEquals(e.getMessage(), "no available client.");
    }
  }
}