/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.mpp.rpc.thrift.TPlanNode;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendBatchPlanNodeResp;
import org.apache.iotdb.mpp.rpc.thrift.TSendSinglePlanNodeReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestInsertPlanValidatorTest {
  @Test
  public void testBatchReportsMissingRegionWrongRegionAndMalformedPlan() throws Exception {
    InsertRowNode row =
        new InsertRowNode(
            new PlanNodeId("test"),
            new PartialPath("root.test.d"),
            false,
            new String[] {"s"},
            new TSDataType[] {TSDataType.INT64},
            1,
            new Object[] {1L},
            false);
    TSendBatchPlanNodeResp response =
        TestInsertPlanValidator.validate(
            new TSendBatchPlanNodeReq(
                Arrays.asList(
                    new TSendSinglePlanNodeReq(
                        new TPlanNode(row.serializeToByteBuffer()),
                        new TConsensusGroupId(TConsensusGroupType.DataRegion, Integer.MAX_VALUE)),
                    new TSendSinglePlanNodeReq(
                        new TPlanNode(row.serializeToByteBuffer()),
                        new TConsensusGroupId(TConsensusGroupType.SchemaRegion, Integer.MAX_VALUE)),
                    new TSendSinglePlanNodeReq(
                        new TPlanNode(ByteBuffer.allocate(0)),
                        new TConsensusGroupId(
                            TConsensusGroupType.DataRegion, Integer.MAX_VALUE)))));
    assertEquals(3, response.getResponsesSize());
    response.getResponses().forEach(result -> assertFalse(result.isAccepted()));
    assertEquals(
        TSStatusCode.NO_AVAILABLE_REGION_GROUP.getStatusCode(),
        response.getResponses().get(0).getStatus().getCode());
    assertEquals(
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
        response.getResponses().get(1).getStatus().getCode());
    assertEquals(
        TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(),
        response.getResponses().get(2).getStatus().getCode());
  }
}
