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

package org.apache.iotdb.db.mpp.plan.analyze.schema;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.mpp.plan.Coordinator;
import org.apache.iotdb.db.mpp.plan.execution.ExecutionResult;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.internal.SchemaFetchStatement;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

class ClusterSchemaFetchExecutor {

  private final Coordinator coordinator;
  private final Supplier<Long> queryIdProvider;
  private final BiFunction<Long, Statement, ExecutionResult> statementExecutor;
  private final Function<PartialPath, Map<Integer, Template>> templateSetInfoProvider;

  private final Map<PartialPath, Pair<AtomicInteger, ClusterSchemaTree>> fetchedResultMap =
      new ConcurrentHashMap<>();

  private final Map<PartialPath, DeviceSchemaFetchTask> executingTaskMap =
      new ConcurrentHashMap<>();

  private final Map<PartialPath, DeviceSchemaFetchTask> waitingTaskMap = new ConcurrentHashMap<>();

  ClusterSchemaFetchExecutor(
      Coordinator coordinator,
      Supplier<Long> queryIdProvider,
      BiFunction<Long, Statement, ExecutionResult> statementExecutor,
      Function<PartialPath, Map<Integer, Template>> templateSetInfoProvider) {
    this.coordinator = coordinator;
    this.queryIdProvider = queryIdProvider;
    this.statementExecutor = statementExecutor;
    this.templateSetInfoProvider = templateSetInfoProvider;
  }

  ClusterSchemaTree fetchSchemaOfOneDevice(PartialPath devicePath, List<String> measurements) {

    final AtomicBoolean shouldWait = new AtomicBoolean(true);
    executingTaskMap.compute(
        devicePath,
        (key, value) -> {
          if (value == null) {
            return null;
          }
          shouldWait.set(value.checkAndAddWaitingThread(measurements));
          return value;
        });

    if (!shouldWait.get()) {
      DeviceSchemaFetchTask task =
          waitingTaskMap.compute(
              devicePath,
              (key, value) -> {
                if (value == null) {
                  value = new DeviceSchemaFetchTask();
                }
                value.addWaitingThread(measurements);
                return value;
              });
      if (executingTaskMap.get(devicePath) != task) {
        synchronized (task) {
          if (executingTaskMap.get(devicePath) != task) {
            while (true) {
              if (executingTaskMap.computeIfAbsent(devicePath, key -> task) != task) {
                break;
              }
            }
            PathPatternTree patternTree = new PathPatternTree();
            for (String measurement : task.measurementSet) {
              patternTree.appendFullPath(devicePath, measurement);
            }
            ClusterSchemaTree fetchedSchemaTree =
                executeSchemaFetchQuery(
                    new SchemaFetchStatement(
                        patternTree, templateSetInfoProvider.apply(devicePath), false));
            Pair<AtomicInteger, ClusterSchemaTree> fetchSchemaResult =
                new Pair<>(new AtomicInteger(task.waitingThreadNum), fetchedSchemaTree);
            while (true) {
              if (fetchedResultMap.computeIfAbsent(devicePath, key -> fetchSchemaResult)
                  != fetchSchemaResult) {
                break;
              }
            }
          }
        }
      }
    }

    while (true) {
      if (!fetchedResultMap.containsKey(devicePath)) {
        break;
      }
    }
    ClusterSchemaTree schemaTree = new ClusterSchemaTree();
    Pair<AtomicInteger, ClusterSchemaTree> pair = fetchedResultMap.get(devicePath);
    if (pair.left.decrementAndGet() == 0) {
      fetchedResultMap.remove(devicePath);
    }

    return null;
  }

  ClusterSchemaTree executeSchemaFetchQuery(SchemaFetchStatement schemaFetchStatement) {
    long queryId = queryIdProvider.get();
    try {
      ExecutionResult executionResult = statementExecutor.apply(queryId, schemaFetchStatement);
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new RuntimeException(
            String.format(
                "cannot fetch schema, status is: %s, msg is: %s",
                executionResult.status.getCode(), executionResult.status.getMessage()));
      }
      try (SetThreadName threadName = new SetThreadName(executionResult.queryId.getId())) {
        ClusterSchemaTree result = new ClusterSchemaTree();
        Set<String> databaseSet = new HashSet<>();
        while (coordinator.getQueryExecution(queryId).hasNextResult()) {
          // The query will be transited to FINISHED when invoking getBatchResult() at the last time
          // So we don't need to clean up it manually
          Optional<TsBlock> tsBlock;
          try {
            tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
          } catch (IoTDBException e) {
            throw new RuntimeException("Fetch Schema failed. ", e);
          }
          if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
            break;
          }
          Column column = tsBlock.get().getColumn(0);
          for (int i = 0; i < column.getPositionCount(); i++) {
            parseFetchedData(column.getBinary(i), result, databaseSet);
          }
        }
        result.setDatabases(databaseSet);
        return result;
      }
    } finally {
      coordinator.cleanupQueryExecution(queryId);
    }
  }

  private void parseFetchedData(
      Binary data, ClusterSchemaTree resultSchemaTree, Set<String> databaseSet) {
    InputStream inputStream = new ByteArrayInputStream(data.getValues());
    try {
      byte type = ReadWriteIOUtils.readByte(inputStream);
      if (type == 0) {
        int size = ReadWriteIOUtils.readInt(inputStream);
        for (int i = 0; i < size; i++) {
          databaseSet.add(ReadWriteIOUtils.readString(inputStream));
        }
      } else if (type == 1) {
        resultSchemaTree.mergeSchemaTree(ClusterSchemaTree.deserialize(inputStream));
      } else {
        throw new RuntimeException(
            new MetadataException("Failed to fetch schema because of unrecognized data"));
      }
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
    }
  }

  private static class DeviceSchemaFetchTask {

    private int waitingThreadNum = 0;

    private final Set<String> measurementSet = new HashSet<>();

    private boolean checkAndAddWaitingThread(List<String> measurements) {
      if (measurementSet.size() < measurements.size()) {
        return false;
      }
      for (String measurement : measurements) {
        if (!measurementSet.contains(measurement)) {
          return false;
        }
      }
      waitingThreadNum++;
      return true;
    }

    private void addWaitingThread(List<String> measurements) {
      waitingThreadNum++;
      measurementSet.addAll(measurements);
    }
  }
}
