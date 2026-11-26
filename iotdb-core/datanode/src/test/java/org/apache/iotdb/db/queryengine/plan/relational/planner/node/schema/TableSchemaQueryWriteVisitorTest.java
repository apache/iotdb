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

package org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.db.queryengine.execution.executor.RegionExecutionResult;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceSourceNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TableSchemaQueryWriteVisitorTest {

  private static final SchemaRegionId SCHEMA_REGION_ID = new SchemaRegionId(1);

  @Test
  public void testRetryableWriteFailureTriggersQueryRetry() {
    final RegionWriteExecutor regionWriteExecutor = mock(RegionWriteExecutor.class);
    final TableDeviceSourceNode node = mock(TableDeviceSourceNode.class);
    when(node.getSenderLocation()).thenReturn(new TDataNodeLocation());

    final TSStatus status =
        new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage("LeaderSteppingDownException");
    final RegionExecutionResult writeResult =
        RegionExecutionResult.create(false, status.getMessage(), status);
    when(regionWriteExecutor.execute(eq(SCHEMA_REGION_ID), any(TableNodeLocationAddNode.class)))
        .thenReturn(writeResult);

    final RegionExecutionResult result =
        new TableSchemaQueryWriteVisitor(regionWriteExecutor)
            .visitTableDeviceSourceNode(node, SCHEMA_REGION_ID);

    assertSame(writeResult, result);
    assertTrue(result.isReadNeedRetry());
    assertEquals(TSStatusCode.DISPATCH_ERROR.getStatusCode(), result.getStatus().getCode());
    assertEquals("LeaderSteppingDownException", result.getStatus().getMessage());
  }

  @Test
  public void testNonRetryableWriteFailureIsPreserved() {
    final RegionWriteExecutor regionWriteExecutor = mock(RegionWriteExecutor.class);
    final TableDeviceSourceNode node = mock(TableDeviceSourceNode.class);
    when(node.getSenderLocation()).thenReturn(new TDataNodeLocation());

    final TSStatus status = new TSStatus(TSStatusCode.SEMANTIC_ERROR.getStatusCode());
    final RegionExecutionResult writeResult =
        RegionExecutionResult.create(false, "semantic error", status);
    when(regionWriteExecutor.execute(eq(SCHEMA_REGION_ID), any(TableNodeLocationAddNode.class)))
        .thenReturn(writeResult);

    final RegionExecutionResult result =
        new TableSchemaQueryWriteVisitor(regionWriteExecutor)
            .visitTableDeviceSourceNode(node, SCHEMA_REGION_ID);

    assertSame(writeResult, result);
    assertFalse(result.isReadNeedRetry());
    assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), result.getStatus().getCode());
  }
}
