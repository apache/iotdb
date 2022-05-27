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

package org.apache.iotdb.db.mpp.plan.analyze;

import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.RegionReplicaSetInfo;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.crud.DeleteDataStatement;
import org.apache.iotdb.db.mpp.plan.statement.internal.InvalidateSchemaCacheStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.List;

public class DeleteTimeseriesDriver {

  public static void processSchemaCacheAndData(
      List<Pair<RegionReplicaSetInfo, List<PartialPath>>> regionRequestList,
      DataPartition dataPartition) {
    executeInvalidateSchemaCache(regionRequestList, dataPartition);
    executeDeleteData(regionRequestList, dataPartition);
  }

  private static void executeInvalidateSchemaCache(
      List<Pair<RegionReplicaSetInfo, List<PartialPath>>> regionRequestList,
      DataPartition dataPartition) {
    InvalidateSchemaCacheStatement statement =
        new InvalidateSchemaCacheStatement(regionRequestList, dataPartition);
    executeStatement(statement);
  }

  private static void executeDeleteData(
      List<Pair<RegionReplicaSetInfo, List<PartialPath>>> regionRequestList,
      DataPartition dataPartition) {
    DeleteDataStatement deleteDataStatement = new DeleteDataStatement();
    deleteDataStatement.setRegionRequestList(regionRequestList);
    deleteDataStatement.setDataPartition(dataPartition);

    executeStatement(deleteDataStatement);
  }

  private static void executeStatement(Statement statement) {
    long queryId = SessionManager.getInstance().requestQueryId(false);
    ExecutionResult executionResult =
        Coordinator.getInstance()
            .execute(
                statement,
                queryId,
                null,
                "",
                ClusterPartitionFetcher.getInstance(),
                ClusterSchemaFetcher.getInstance());

    if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // todo exception handle
      throw new RuntimeException(
          String.format(
              "cannot delete timeseries , status is: %s, msg is: %s",
              executionResult.status.getCode(), executionResult.status.getMessage()));
    }
  }
}
