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

package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.db.mpp.common.object.entry.SchemaFetchObjectEntry;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.object.ObjectSourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class SchemaFetchSGScanOperator extends ObjectSourceOperator<SchemaFetchObjectEntry> {

  private final List<String> storageGroupList;

  private boolean isFinished = false;

  public SchemaFetchSGScanOperator(
      PlanNodeId sourceId,
      OperatorContext operatorContext,
      String queryId,
      List<String> storageGroupList) {
    super(sourceId, operatorContext, queryId);
    this.storageGroupList = storageGroupList;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return NOT_BLOCKED;
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateMaxReturnSize() {
    return DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  @Override
  protected boolean hasNextBatch() {
    return !isFinished;
  }

  @Override
  protected List<SchemaFetchObjectEntry> nextBatch() {
    isFinished = true;
    return Collections.singletonList(new SchemaFetchObjectEntry(storageGroupList));
  }
}
