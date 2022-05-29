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
package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ISchemaFetcher;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.internal.LastPointFetchStatement;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SchemaQueryAddLastPointOperator implements ProcessOperator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SchemaQueryAddLastPointOperator.class);

  private final long queryId = SessionManager.getInstance().requestQueryId(false);
  private final Coordinator coordinator = Coordinator.getInstance();
  private final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();
  private final ISchemaFetcher schemaFetcher = ClusterSchemaFetcher.getInstance();

  private final OperatorContext operatorContext;
  private final Operator child;
  private boolean isFinished;

  public SchemaQueryAddLastPointOperator(OperatorContext operatorContext, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    this.child = requireNonNull(child, "child operator is null");
    isFinished = false;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() {
    isFinished = true;
    TsBlock block = child.next();
    TsBlockBuilder builder =
        new TsBlockBuilder(HeaderConstant.showTimeSeriesWithLatestHeader.getRespDataTypes());
    if (null != block) {
      // Step 1: get paths and storage groups
      List<MeasurementPath> measurementPaths = new LinkedList<>();
      List<String> storageGroups = new LinkedList<>();
      for (int i = 0; i < block.getPositionCount(); i++) {
        String path = block.getColumn(0).getBinary(i).toString();
        String storageGroup = block.getColumn(2).getBinary(i).toString();
        MeasurementPath measurementPath;
        try {
          measurementPath = new MeasurementPath(path);
        } catch (MetadataException e) {
          LOGGER.error("Failed to convert {} to measurementPath", path);
          continue;
        }
        measurementPaths.add(measurementPath);
        storageGroups.add(storageGroup);
      }
      // Step 2: fetch last point
      LastPointFetchStatement lastPointFetchStatement =
          new LastPointFetchStatement(measurementPaths, storageGroups);
      ExecutionResult executionResult =
          coordinator.execute(
              lastPointFetchStatement, queryId, null, "", partitionFetcher, schemaFetcher);
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            String.format(
                "cannot fetch last point, status is: %s, msg is: %s",
                executionResult.status.getCode(), executionResult.status.getMessage()));
      }
      Map<String, String> timeseriesToLastPoint = new HashMap<>();
      while (coordinator.getQueryExecution(queryId).hasNextResult()) {
        // The query will be transited to FINISHED when invoking getBatchResult() at the last time
        // So we don't need to clean up it manually
        Optional<TsBlock> tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
        if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
          break;
        }
        for (int i = 0; i < tsBlock.get().getPositionCount(); i++) {
          String timeseries = tsBlock.get().getColumn(0).getBinary(i).toString();
          String value = tsBlock.get().getColumn(1).getBinary(i).toString();
          timeseriesToLastPoint.put(timeseries, value);
        }
      }
      // Step 3: generate result
      for (int i = 0; i < block.getPositionCount(); i++) {
        builder.getTimeColumnBuilder().writeLong(0L);
        String timeseries = block.getColumn(0).getBinary(i).toString();
        for (int j = 0; j < block.getValueColumnCount(); j++) {
          builder.getColumnBuilder(j).writeBinary(block.getColumn(j).getBinary(i));
        }
        Binary binary = new Binary("0");
        if (timeseriesToLastPoint.containsKey(timeseries)) {
          binary = new Binary(timeseriesToLastPoint.get(timeseries));
        }
        builder.getColumnBuilder(8).writeBinary(binary);
        builder.declarePosition();
      }
    }

    return builder.build();
  }

  @Override
  public boolean hasNext() {
    return child.hasNext();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() {
    return isFinished || child.isFinished();
  }
}
