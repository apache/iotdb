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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.write;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.common.request.NormalizedRequest;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.List;
import java.util.Map;

/** Shared row handling for the two tree-model batch plan nodes. */
final class ResearchInsertNormalizer {
  private ResearchInsertNormalizer() {}

  static NormalizedRequest rows(
      InsertNode parent,
      Class<? extends InsertNode> supportedClass,
      List<InsertRowNode> rows,
      List<Integer> sourceIndexes,
      Map<Integer, TSStatus> results) {
    if (parent.getClass() != supportedClass || parent.isGeneratedByPipe() || rows.isEmpty()) {
      return NormalizedRequest.single(parent);
    }
    for (InsertRowNode row : rows) {
      // Table-model, pipe and unknown extensions retain their complete application operation.
      if (row == null || row.getClass() != InsertRowNode.class || row.isGeneratedByPipe()) {
        return NormalizedRequest.single(parent);
      }
    }
    if (sourceIndexes.size() != rows.size()) {
      throw new IllegalArgumentException("InsertRows row/index count mismatch");
    }
    results.clear();
    return new NormalizedRequest(
        rows.size(),
        index -> copyRow(rows.get(index)),
        sourceIndexes::get,
        NormalizedRequest.FailureSelection.FIRST,
        (index, status) -> {
          if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            results.put(index, status);
          }
        });
  }

  private static InsertRowNode copyRow(InsertRowNode source) {
    InsertRowNode result =
        new InsertRowNode(
            source.getPlanNodeId(),
            source.targetPath,
            source.isAligned,
            source.measurements.clone(),
            source.dataTypes == null ? null : source.dataTypes.clone(),
            source.measurementSchemas == null ? null : source.measurementSchemas.clone(),
            source.getTime(),
            source.getValues().clone(),
            source.isNeedInferType());
    result.failedMeasurementNumber = source.failedMeasurementNumber;
    result.dataRegionReplicaSet = source.dataRegionReplicaSet;
    result.deviceID = source.deviceID;
    result.progressIndex = source.progressIndex;
    result.searchIndex = source.searchIndex;
    result.isGeneratedByRemoteConsensusLeader = source.isGeneratedByRemoteConsensusLeader;
    return result;
  }
}
