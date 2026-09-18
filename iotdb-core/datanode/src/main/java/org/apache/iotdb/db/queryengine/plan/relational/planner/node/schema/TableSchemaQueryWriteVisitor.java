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

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.utils.RetryUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.execution.executor.RegionExecutionResult;
import org.apache.iotdb.db.queryengine.execution.executor.RegionWriteExecutor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.read.TableDeviceSourceNode;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Objects;

public class TableSchemaQueryWriteVisitor
    extends AbstractTableSchemaQueryAttributeSecurityVisitor<RegionExecutionResult> {

  private final RegionWriteExecutor regionWriteExecutor;

  public TableSchemaQueryWriteVisitor() {
    this(new RegionWriteExecutor());
  }

  @TestOnly
  public TableSchemaQueryWriteVisitor(final RegionWriteExecutor regionWriteExecutor) {
    this.regionWriteExecutor = regionWriteExecutor;
  }

  @Override
  protected RegionExecutionResult visitTableDeviceSourceNode(
      final TableDeviceSourceNode node, final ConsensusGroupId context) {
    if (Objects.nonNull(node.getSenderLocation())) {
      final RegionExecutionResult result =
          regionWriteExecutor.execute(
              context, new TableNodeLocationAddNode(new PlanNodeId(""), node.getSenderLocation()));
      if (!result.isAccepted()
          && Objects.nonNull(result.getStatus())
          && RetryUtils.needRetryForWrite(result.getStatus().getCode())) {
        // This write is an internal step of a schema query. Convert a retryable write failure to a
        // dispatch failure so that the query scheduler retries the query instead of reporting the
        // transient consensus error to the client.
        result.setReadNeedRetry(true);
        result.getStatus().setCode(TSStatusCode.DISPATCH_ERROR.getStatusCode());
      }
      return result.isAccepted() ? null : result;
    }
    return null;
  }
}
