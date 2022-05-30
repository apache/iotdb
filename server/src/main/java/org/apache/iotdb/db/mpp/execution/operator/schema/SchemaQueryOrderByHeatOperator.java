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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class SchemaQueryOrderByHeatOperator implements ProcessOperator {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SchemaQueryOrderByHeatOperator.class);

  private final long queryId = SessionManager.getInstance().requestQueryId(false);
  private final Coordinator coordinator = Coordinator.getInstance();
  private final IPartitionFetcher partitionFetcher = ClusterPartitionFetcher.getInstance();
  private final ISchemaFetcher schemaFetcher = ClusterSchemaFetcher.getInstance();

  private final OperatorContext operatorContext;
  private final List<Operator> children;
  private final boolean[] noMoreTsBlocks;
  private boolean isFinished;

  public SchemaQueryOrderByHeatOperator(OperatorContext operatorContext, List<Operator> children) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    this.children = children;
    isFinished = false;
    noMoreTsBlocks = new boolean[children.size()];
  }

  @Override
  public TsBlock next() {
    isFinished = true;

    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(HeaderConstant.showTimeSeriesHeader.getRespDataTypes());
    // last timestamp to timeseries
    Map<Long, List<Object[]>> lastTimeToTimeseries = new HashMap<>();

    TsBlock tsblock = children.get(1).next();
    while (children.get(0).hasNext()) {
      TsBlock block = children.get(0).next();
      if (null != block) {
        // Step 1: get paths and storage groups
        List<MeasurementPath> measurementPaths = new LinkedList<>();
        Set<String> storageGroups = new HashSet<>();
        for (int i = 0; i < block.getPositionCount(); i++) {
          String path = block.getColumn(0).getBinary(i).toString();
          String storageGroup = block.getColumn(2).getBinary(i).toString();
          String dataType = block.getColumn(3).getBinary(i).toString();
          MeasurementPath measurementPath;
          try {
            measurementPath = new MeasurementPath(path, TSDataType.valueOf(dataType));
          } catch (MetadataException e) {
            LOGGER.error("Failed to convert {} to measurementPath", path);
            continue;
          }
          measurementPaths.add(measurementPath);
          storageGroups.add(storageGroup);
        }

        // Step 2: fetch last point
        LastPointFetchStatement lastPointFetchStatement =
            new LastPointFetchStatement(measurementPaths, new ArrayList<>(storageGroups));
        ExecutionResult executionResult =
            coordinator.execute(
                lastPointFetchStatement, queryId, null, "", partitionFetcher, schemaFetcher);
        if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new RuntimeException(
              String.format(
                  "cannot fetch last point, status is: %s, msg is: %s",
                  executionResult.status.getCode(), executionResult.status.getMessage()));
        }

        Map<String, Long> timeseriesToLastStamp = new HashMap<>();
        while (coordinator.getQueryExecution(queryId).hasNextResult()) {
          // The query will be transited to FINISHED when invoking getBatchResult() at the last time
          // So we don't need to clean up it manually
          Optional<TsBlock> tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
          if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
            break;
          }
          for (int i = 0; i < tsBlock.get().getPositionCount(); i++) {
            String timeseries = tsBlock.get().getColumn(0).getBinary(i).toString();
            long time = tsBlock.get().getTimeByIndex(i);
            timeseriesToLastStamp.put(timeseries, time);
          }
        }

        // Step 3: merge according to result
        TsBlock.TsBlockRowIterator tsBlockRowIterator = block.getTsBlockRowIterator();
        while (tsBlockRowIterator.hasNext()) {
          Object[] line = tsBlockRowIterator.next();
          String timeseries = line[0].toString();
          long time = timeseriesToLastStamp.getOrDefault(timeseries, 0L);
          if (!lastTimeToTimeseries.containsKey(time)) {
            lastTimeToTimeseries.put(time, new ArrayList<>());
          }
          lastTimeToTimeseries.get(time).add(line);
        }
      }
    }

    // Step 4: sort and rewrite
    List<Long> times = new ArrayList<>(lastTimeToTimeseries.keySet());
    times.sort(
        (l1, l2) -> {
          if (l1 > l2) {
            return -1;
          } else if (l1 < l2) {
            return 1;
          } else {
            return 0;
          }
        });
    for (Long time : times) {
      List<Object[]> rows = lastTimeToTimeseries.get(time);
      for (Object[] row : rows) {
        tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
        for (int i = 0; i < HeaderConstant.showTimeSeriesHeader.getRespDataTypes().size(); i++) {
          Object value = row[i];
          if (null == value) {
            tsBlockBuilder.getColumnBuilder(i).appendNull();
          } else {
            tsBlockBuilder.getColumnBuilder(i).writeBinary(new Binary(value.toString()));
          }
        }
        tsBlockBuilder.declarePosition();
      }
    }

    return tsBlockBuilder.build();
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    for (int i = 0; i < children.size(); i++) {
      ListenableFuture<Void> blocked = children.get(i).isBlocked();
      if (!blocked.isDone()) {
        return blocked;
      }
    }
    return NOT_BLOCKED;
  }

  @Override
  public boolean hasNext() {
    return !isFinished;
  }

  @Override
  public void close() throws Exception {
    for (Operator child : children) {
      child.close();
    }
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }
}
