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
package org.apache.iotdb.db.queryengine.plan.analyze;

import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.planner.LocalExecutionPlanner;
import org.apache.iotdb.rpc.TElasticFramedTransport;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class QueryPartitionMemoryTest {

  private int originalDataNodeId;

  @Before
  public void setUp() {
    originalDataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalDataNodeId);
  }

  @Test
  public void testOversizedPartitionRequestReleasesFrontEndMemory() {
    LocalExecutionPlanner planner = LocalExecutionPlanner.getInstance();
    long freeMemory = planner.getFreeMemoryForOperators();
    String sql =
        "select s1 from root.sg.d1 where time >= 0 and time <= "
            + TimePartitionUtils.getTimePartitionInterval() * 50_000;
    for (long queryId = 100; queryId < 103; queryId++) {
      final int[] serializedBytes = {0};
      IPartitionFetcher partitionFetcher =
          new FakePartitionFetcherImpl() {
            @Override
            public DataPartition getDataPartition(
                Map<String, List<DataPartitionQueryParam>> params) {
              assertTrue(planner.getFreeMemoryForOperators() < freeMemory);
              DataPartitionQueryParam param = params.get("root.sg").get(0);
              TTimeSlotList slots =
                  new TTimeSlotList(param.getTimePartitionSlotList(), false, false);
              TDataPartitionReq request =
                  new TDataPartitionReq(
                      Collections.singletonMap(
                          "root.sg", Collections.singletonMap(new TSeriesPartitionSlot(0), slots)));
              try (TElasticFramedTransport transport =
                  new TElasticFramedTransport(new TMemoryBuffer(0), 16, 1024, false) {
                    @Override
                    public void close() {
                      serializedBytes[0] = writeBuffer.getPos();
                      super.close();
                    }
                  }) {
                new IConfigNodeRPCService.Client(new TBinaryProtocol(transport))
                    .send_getDataPartitionTable(request);
                fail("The partition request must exceed the frame size limit");
              } catch (TException e) {
                throw new StatementAnalyzeException(e.getMessage());
              }
              return null;
            }
          };
      try {
        Coordinator.getInstance()
            .executeForTreeModel(
                StatementGenerator.createStatement(sql, ZoneOffset.UTC),
                queryId,
                new SessionInfo(0, "root", ZoneOffset.UTC, ""),
                sql,
                partitionFetcher,
                new FakeSchemaFetcherImpl(),
                60_000,
                true,
                false);
        fail("The oversized partition request must fail query analysis");
      } catch (StatementAnalyzeException e) {
        assertTrue(e.getMessage(), e.getMessage().contains("larger than protect max size (1024)"));
      }
      assertTrue(serializedBytes[0] > 0);
      assertTrue("Serialization must stop at the frame limit", serializedBytes[0] <= 1024);
      assertEquals(freeMemory, planner.getFreeMemoryForOperators());
      assertNull(Coordinator.getInstance().getQueryExecution(queryId));
    }
  }
}
