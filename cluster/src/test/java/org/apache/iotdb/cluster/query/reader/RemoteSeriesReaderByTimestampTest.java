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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.cluster.client.DataClient;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.SerializeUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.thrift.async.AsyncMethodCallback;
import org.junit.Test;

public class RemoteSeriesReaderByTimestampTest {

  private BatchData batchData = TestUtils.genBatchData(TSDataType.DOUBLE, 0, 100);

  private MetaGroupMember metaGroupMember = new MetaGroupMember() {
    @Override
    public DataClient getDataClient(Node node) throws IOException {
      return new DataClient(null, null, node, null) {
        @Override
        public void fetchSingleSeriesByTimestamp(Node header, long readerId, long timestamp,
            AsyncMethodCallback<ByteBuffer> resultHandler) {
          new Thread(() -> {
            while (batchData.hasCurrent()) {
              long time = batchData.currentTime();
              Object value = batchData.currentValue();
              if (time == timestamp) {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
                SerializeUtils.serializeObject(value, dataOutputStream);
                resultHandler.onComplete(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
                batchData.next();
                return;
              } else if (time > timestamp) {
                resultHandler.onComplete(ByteBuffer.allocate(0));
                break;
              }
              // time < timestamp, continue
              batchData.next();
            }
            resultHandler.onComplete(ByteBuffer.allocate(0));
          }).start();
        }
      };
    }
  };

  @Test
  public void test() throws IOException {
    RemoteSeriesReaderByTimestamp reader = new RemoteSeriesReaderByTimestamp(0,
        TestUtils.getNode(1), TestUtils.getNode(0),
        metaGroupMember);
    for (int i = 0; i < 100; i++) {
      assertEquals(i * 1.0, reader.getValueInTimestamp(i));
    }
    assertNull(reader.getValueInTimestamp(101));
  }
}