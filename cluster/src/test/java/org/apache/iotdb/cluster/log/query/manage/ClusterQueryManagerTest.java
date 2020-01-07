/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at      http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.cluster.log.query.manage;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.cluster.query.manage.ClusterQueryManager;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.junit.Before;
import org.junit.Test;

public class ClusterQueryManagerTest {

  private ClusterQueryManager queryManager;

  @Before
  public void setUp() {
    queryManager = new ClusterQueryManager();
  }

  @Test
  public void testContext() {
    RemoteQueryContext queryContext1 = queryManager.getQueryContext(TestUtils.getNode(0), 1);
    RemoteQueryContext queryContext2 = queryManager.getQueryContext(TestUtils.getNode(0), 1);
    RemoteQueryContext queryContext3 = queryManager.getQueryContext(TestUtils.getNode(1), 1);
    assertSame(queryContext1, queryContext2);
    assertNotEquals(queryContext2, queryContext3);
  }

  @Test
  public void testRegisterReader() {
    IBatchReader reader = new IBatchReader() {
      @Override
      public boolean hasNextBatch() {
        return false;
      }

      @Override
      public BatchData nextBatch() {
        return null;
      }

      @Override
      public void close() {

      }
    };
    long id = queryManager.registerReader(reader);
    assertSame(reader, queryManager.getReader(id));
  }

  @Test
  public void testRegisterReaderByTime() {
    IReaderByTimestamp reader = new IReaderByTimestamp() {
      @Override
      public Object getValueInTimestamp(long timestamp) {
        return null;
      }

      @Override
      public boolean hasNext() {
        return false;
      }
    };
    long id = queryManager.registerReaderByTime(reader);
    assertSame(reader, queryManager.getReaderByTimestamp(id));
  }

  @Test
  public void testEndQuery() throws StorageEngineException {
    RemoteQueryContext queryContext = queryManager.getQueryContext(TestUtils.getNode(0), 1);
    for (int i = 0; i < 10; i++) {
      IBatchReader reader = new IBatchReader() {
        @Override
        public boolean hasNextBatch() {
          return false;
        }

        @Override
        public BatchData nextBatch() {
          return null;
        }

        @Override
        public void close() {

        }
      };
      queryContext.registerLocalReader(queryManager.registerReader(reader));
    }
    queryManager.endQuery(TestUtils.getNode(0), 1);
    for (int i = 0; i < 10; i++) {
      assertNull(queryManager.getReader(i));
    }
  }
}