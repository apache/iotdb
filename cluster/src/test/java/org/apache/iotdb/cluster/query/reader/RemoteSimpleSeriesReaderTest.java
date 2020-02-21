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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.common.TestMetaGroupMember;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Before;
import org.junit.Test;

public class RemoteSimpleSeriesReaderTest {

  private RemoteSimpleSeriesReader reader;
  private DataClient client;
  private BatchData batchData;
  private boolean batchUsed;

  @Before
  public void setUp() throws IOException {
    batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);
    batchUsed = false;
    client = new DataClient(null, null, TestUtils.getNode(0), null){
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
            resultHandler.onComplete(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
            batchUsed = true;
          }
        }).start();
      }
    };
    MetaGroupMember metaGroupMember = new TestMetaGroupMember() {

      @Override
      public DataClient getDataClient(Node node) {
        return client;
      }
    };
    reader = new RemoteSimpleSeriesReader(0, TestUtils.getNode(1), TestUtils.getNode(0),
        metaGroupMember);
  }

  @Test
  public void testBatch() throws IOException {
    assertTrue(reader.hasNextBatch());
    assertTrue(TestUtils.batchEquals(batchData, reader.nextBatch()));
    assertFalse(reader.hasNextBatch());
  }

  @Test
  public void testSingle() throws IOException {
    for (int i = 0; i < 100; i++) {
      assertTrue(reader.hasNext());
      TimeValuePair curr = reader.current();
      TimeValuePair pair = reader.next();
      assertEquals(pair, curr);
      assertEquals(i, pair.getTimestamp());
      assertEquals(i * 1.0, pair.getValue().getDouble(), 0.00001);
    }
    assertFalse(reader.hasNext());
  }
}