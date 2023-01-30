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

package org.apache.iotdb.db.mpp.plan.analyze.schema;

import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.metadata.template.ITemplateManager;
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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

class ClusterSchemaFetchExecutor {

  private final Coordinator coordinator;
  private final ITemplateManager templateManager;
  private final Supplier<Long> queryIdProvider;
  private final BiFunction<Long, Statement, ExecutionResult> statementExecutor;
  private final Consumer<ClusterSchemaTree> schemaCacheUpdater;

  ClusterSchemaFetchExecutor(
      Coordinator coordinator,
      ITemplateManager templateManager,
      Supplier<Long> queryIdProvider,
      BiFunction<Long, Statement, ExecutionResult> statementExecutor,
      Consumer<ClusterSchemaTree> schemaCacheUpdater) {
    this.coordinator = coordinator;
    this.templateManager = templateManager;
    this.queryIdProvider = queryIdProvider;
    this.statementExecutor = statementExecutor;
    this.schemaCacheUpdater = schemaCacheUpdater;
  }

  /**
   * This method is used for scenarios that patternTree may have wildcard or there's no need to
   * cache the result.
   */
  ClusterSchemaTree fetchSchemaOfFuzzyMatch(PathPatternTree patternTree, boolean withTags) {
    Map<Integer, Template> templateMap = new HashMap<>();
    List<PartialPath> pathPatternList = patternTree.getAllPathPatterns();
    for (PartialPath pattern : pathPatternList) {
      templateMap.putAll(templateManager.checkAllRelatedTemplate(pattern));
    }
    return executeSchemaFetchQuery(new SchemaFetchStatement(patternTree, templateMap, withTags));
  }

  /**
   * This method is used for scenarios that patternTree has no wildcard and the result should be
   * cached.
   *
   * @param fullPathList all the fullPath without wildcard split from rawPatternTree
   * @param rawPatternTree the pattern tree consisting of the fullPathList
   * @return fetched schema
   */
  ClusterSchemaTree fetchSchemaOfPreciseMatch(
      List<PartialPath> fullPathList, PathPatternTree rawPatternTree) {
    ClusterSchemaTree schemaTree =
        executeSchemaFetchQuery(
            new SchemaFetchStatement(rawPatternTree, analyzeTemplate(fullPathList), false));
    if (!schemaTree.isEmpty()) {
      schemaCacheUpdater.accept(schemaTree);
    }
    return schemaTree;
  }

  ClusterSchemaTree fetchSchemaOfOneDevice(
      PartialPath devicePath, String[] measurements, List<Integer> indexOfTargetMeasurements) {
    PathPatternTree patternTree = new PathPatternTree();
    for (int index : indexOfTargetMeasurements) {
      patternTree.appendFullPath(devicePath, measurements[index]);
    }
    patternTree.constructTree();
    return fetchSchemaAndCacheResult(patternTree);
  }

  ClusterSchemaTree fetchSchemaOfMultiDevices(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<Integer> indexOfTargetDevices,
      List<List<Integer>> indexOfTargetMeasurementsList) {
    PathPatternTree patternTree = new PathPatternTree();
    int deviceIndex;
    for (int i = 0, size = indexOfTargetDevices.size(); i < size; i++) {
      deviceIndex = indexOfTargetDevices.get(i);
      for (int measurementIndex : indexOfTargetMeasurementsList.get(i)) {
        patternTree.appendFullPath(
            devicePathList.get(deviceIndex), measurementsList.get(deviceIndex)[measurementIndex]);
      }
    }
    patternTree.constructTree();
    return fetchSchemaAndCacheResult(patternTree);
  }

  private ClusterSchemaTree fetchSchemaAndCacheResult(PathPatternTree patternTree) {
    ClusterSchemaTree schemaTree =
        executeSchemaFetchQuery(
            new SchemaFetchStatement(
                patternTree, analyzeTemplate(patternTree.getAllPathPatterns()), false));
    if (!schemaTree.isEmpty()) {
      schemaCacheUpdater.accept(schemaTree);
    }
    return schemaTree;
  }

  private Map<Integer, Template> analyzeTemplate(List<PartialPath> pathPatternList) {
    Map<Integer, Template> templateMap = new HashMap<>();
    for (PartialPath pattern : pathPatternList) {
      templateMap.putAll(templateManager.checkAllRelatedTemplate(pattern));
    }
    return templateMap;
  }

  private ClusterSchemaTree executeSchemaFetchQuery(SchemaFetchStatement schemaFetchStatement) {
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
}
