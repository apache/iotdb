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

package org.apache.iotdb.cluster.coordinator;

import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.server.member.MetaGroupMemberTest;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.write.schema.TimeseriesSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.apache.iotdb.cluster.server.NodeCharacter.LEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CoordinatorTest extends MetaGroupMemberTest {
  protected Coordinator coordinator;

  @Override
  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Before
  public void setUp() throws Exception {
    coordinator = new Coordinator();
    super.setUp();
    testMetaMember.setCoordinator(coordinator);
    coordinator.setMetaGroupMember(testMetaMember);
    coordinator.setRouter(testMetaMember.getRouter());
  }

  @Test
  public void testProcessNonQuery() throws IllegalPathException {
    System.out.println("Start testProcessNonQuery()");
    mockDataClusterServer = true;
    // as a leader
    testMetaMember.setCharacter(LEADER);
    testMetaMember.setAppendLogThreadPool(testThreadPool);
    for (int i = 10; i < 20; i++) {
      // process a non partitioned plan
      SetStorageGroupPlan setStorageGroupPlan =
        new SetStorageGroupPlan(new PartialPath(TestUtils.getTestSg(i)));
      TSStatus status = coordinator.executeNonQueryPlan(setStorageGroupPlan);
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.code);
      assertTrue(IoTDB.metaManager.isPathExist(new PartialPath(TestUtils.getTestSg(i))));

      // process a partitioned plan
      TimeseriesSchema schema = TestUtils.getTestTimeSeriesSchema(i, 0);
      CreateTimeSeriesPlan createTimeSeriesPlan = new CreateTimeSeriesPlan(
        new PartialPath(schema.getFullPath()), schema.getType(),
        schema.getEncodingType(), schema.getCompressor(), schema.getProps(),
        Collections.emptyMap(), Collections.emptyMap(), null);
      status = testMetaMember.executeNonQueryPlan(createTimeSeriesPlan);
      if (status.getCode() == TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
        status.setCode(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
      assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.code);
      assertTrue(IoTDB.metaManager.isPathExist(new PartialPath(TestUtils.getTestSeries(i, 0))));
    }
    testThreadPool.shutdownNow();
  }

}
