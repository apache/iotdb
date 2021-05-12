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
import org.apache.iotdb.cluster.partition.PartitionGroup;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.SingleSeriesQueryRequest;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;

public class DatasourceInfoTest {
  private MetaGroupMember metaGroupMember;

  @Before
  public void setUp() {
    metaGroupMember = new TestMetaGroupMember();
    metaGroupMember.setClientProvider(
        new DataClientProvider(new Factory()) {
          @Override
          public AsyncDataClient getAsyncDataClient(Node node, int timeout) throws IOException {
            return new AsyncDataClient(null, null, TestUtils.getNode(0), null) {
              @Override
              public void querySingleSeries(
                  SingleSeriesQueryRequest request, AsyncMethodCallback<Long> resultHandler)
                  throws TException {
                throw new TException("Don't worry, this is the exception I constructed.");
              }
            };
          }
        });
  }

  @Test
  public void testFailedAll() throws StorageEngineException {
    PartitionGroup group = new PartitionGroup();
    group.add(TestUtils.getNode(0));
    group.add(TestUtils.getNode(1));
    group.add(TestUtils.getNode(2));

    SingleSeriesQueryRequest request = new SingleSeriesQueryRequest();
    RemoteQueryContext context = new RemoteQueryContext(1);

    try {
      DataSourceInfo sourceInfo =
          new DataSourceInfo(group, TSDataType.DOUBLE, request, context, metaGroupMember, group);
      boolean hasClient = sourceInfo.hasNextDataClient(false, Long.MIN_VALUE);

      assertFalse(hasClient);
    } finally {
      QueryResourceManager.getInstance().endQuery(context.getQueryId());
    }
  }
}
