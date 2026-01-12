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

package org.apache.iotdb.db.queryengine.plan.analyze.schema;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.QuerySchemaFetchFailedException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.schematree.ClusterSchemaTree;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.DeviceSchemaFetchStatement;
import org.apache.iotdb.db.queryengine.plan.statement.internal.SeriesSchemaFetchStatement;
import org.apache.iotdb.db.schemaengine.template.ITemplateManager;
import org.apache.iotdb.db.utils.SetThreadName;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

class ClusterSchemaFetchExecutor {

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final Coordinator coordinator;
  private final ITemplateManager templateManager;
  private final ISchemaFetcher schemaFetcher;
  private final Consumer<ClusterSchemaTree> schemaCacheUpdater;

  ClusterSchemaFetchExecutor(
      Coordinator coordinator,
      ITemplateManager templateManager,
      ISchemaFetcher schemaFetcher,
      Consumer<ClusterSchemaTree> schemaCacheUpdater) {
    this.coordinator = coordinator;
    this.templateManager = templateManager;
    this.schemaFetcher = schemaFetcher;
    this.schemaCacheUpdater = schemaCacheUpdater;
  }

  private ExecutionResult executionStatement(
      final long queryId, final Statement statement, final MPPQueryContext context) {

    final long timeout =
        context == null
            ? config.getQueryTimeoutThreshold()
            : context.getTimeOut() - (System.currentTimeMillis() - context.getStartTime());
    String sql = context == null ? "" : "Fetch Schema for " + context.getQueryType();
    if (context != null && context.isQuery()) {
      sql += ", " + context.getQueryId() + " : " + context.getSql();
    }
    return coordinator.executeForTreeModel(
        statement,
        queryId,
        context == null
            ? null
            : SessionManager.getInstance().copySessionInfoForTreeModel(context.getSession()),
        sql,
        ClusterPartitionFetcher.getInstance(),
        schemaFetcher,
        timeout,
        false);
  }

  /**
   * This method is used for scenarios that patternTree may have wildcard or there's no need to
   * cache the result.
   */
  ClusterSchemaTree fetchSchemaOfFuzzyMatch(
      PathPatternTree patternTree,
      boolean withTags,
      boolean withTemplate,
      MPPQueryContext context,
      boolean canSeeAuditDB) {
    Map<Integer, Template> templateMap = new HashMap<>();
    List<PartialPath> pathPatternList = patternTree.getAllPathPatterns();
    for (PartialPath pattern : pathPatternList) {
      templateMap.putAll(templateManager.checkAllRelatedTemplate(pattern));
    }
    return executeSchemaFetchQuery(
        new SeriesSchemaFetchStatement(
            patternTree, templateMap, withTags, false, withTemplate, false, canSeeAuditDB),
        context);
  }

  /**
   * This method is used for scenarios that patternTree has no wildcard and the result should be
   * cached.
   *
   * @param fullPathList all the fullPath without wildcard split from rawPatternTree
   * @param rawPatternTree the pattern tree consisting of the fullPathList
   * @return fetched schema
   */
  ClusterSchemaTree fetchSchemaOfPreciseMatchOrPreciseDeviceUsingTemplate(
      List<PartialPath> fullPathList,
      PathPatternTree rawPatternTree,
      boolean withTemplate,
      MPPQueryContext context,
      boolean canSeeAuditDB) {
    ClusterSchemaTree schemaTree =
        executeSchemaFetchQuery(
            new SeriesSchemaFetchStatement(
                rawPatternTree,
                analyzeTemplate(fullPathList),
                false,
                withTemplate,
                withTemplate,
                withTemplate,
                canSeeAuditDB),
            context);
    if (!schemaTree.isEmpty()) {
      schemaCacheUpdater.accept(schemaTree);
    }
    return schemaTree;
  }

  ClusterSchemaTree fetchDeviceLevelRawSchema(
      PathPatternTree patternTree,
      PathPatternTree authorityScope,
      MPPQueryContext context,
      boolean canSeeAuditDB) {
    return executeSchemaFetchQuery(
        new DeviceSchemaFetchStatement(patternTree, authorityScope, canSeeAuditDB), context);
  }

  ClusterSchemaTree fetchMeasurementLevelRawSchema(
      PathPatternTree patternTree, MPPQueryContext context, boolean canSeeAuditDB) {
    Map<Integer, Template> templateMap = new HashMap<>();
    List<PartialPath> pathPatternList = patternTree.getAllPathPatterns();
    for (PartialPath pattern : pathPatternList) {
      templateMap.putAll(templateManager.checkAllRelatedTemplate(pattern));
    }
    return executeSchemaFetchQuery(
        new SeriesSchemaFetchStatement(
            patternTree, templateMap, true, true, false, true, canSeeAuditDB),
        context);
  }

  ClusterSchemaTree fetchSchemaOfOneDevice(
      PartialPath devicePath,
      String[] measurements,
      List<Integer> indexOfTargetMeasurements,
      MPPQueryContext context) {
    PathPatternTree patternTree = new PathPatternTree();
    for (int index : indexOfTargetMeasurements) {
      patternTree.appendFullPath(devicePath, measurements[index]);
    }
    patternTree.constructTree();
    return fetchSchemaAndCacheResult(patternTree, context);
  }

  ClusterSchemaTree fetchSchemaOfMultiDevices(
      List<PartialPath> devicePathList,
      List<String[]> measurementsList,
      List<Integer> indexOfTargetDevices,
      List<List<Integer>> indexOfTargetMeasurementsList,
      MPPQueryContext context) {
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
    return fetchSchemaAndCacheResult(patternTree, context);
  }

  ClusterSchemaTree fetchSchemaWithFullPaths(List<String> fullPathList, MPPQueryContext context) {
    PathPatternTree patternTree = new PathPatternTree();
    for (String fullPath : fullPathList) {
      try {
        patternTree.appendFullPath(new MeasurementPath(fullPath));
      } catch (IllegalPathException e) {
        throw new RuntimeException(e);
      }
    }
    patternTree.constructTree();
    return fetchSchemaAndCacheResult(patternTree, context);
  }

  ClusterSchemaTree fetchSchemaWithPatternTreeAndCache(
      PathPatternTree patternTree, MPPQueryContext context) {
    patternTree.constructTree();
    return fetchSchemaAndCacheResult(patternTree, context);
  }

  private ClusterSchemaTree fetchSchemaAndCacheResult(
      PathPatternTree patternTree, MPPQueryContext context) {
    ClusterSchemaTree schemaTree =
        executeSchemaFetchQuery(
            new SeriesSchemaFetchStatement(
                patternTree,
                analyzeTemplate(patternTree.getAllPathPatterns()),
                false,
                false,
                true,
                false,
                true),
            context);
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

  private ClusterSchemaTree executeSchemaFetchQuery(
      Statement fetchStatement, MPPQueryContext context) {
    long queryId = SessionManager.getInstance().requestQueryId();
    Throwable t = null;
    try {
      ExecutionResult executionResult = executionStatement(queryId, fetchStatement, context);
      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new QuerySchemaFetchFailedException(
            String.format("Fetch Schema failed, because %s", executionResult.status.getMessage()),
            executionResult.status.getCode());
      }
      try (SetThreadName ignored = new SetThreadName(executionResult.queryId.getId())) {
        ClusterSchemaTree result = new ClusterSchemaTree();
        ClusterSchemaTree.SchemaNodeBatchDeserializer deserializer =
            new ClusterSchemaTree.SchemaNodeBatchDeserializer();
        Set<String> databaseSet = new HashSet<>();
        while (coordinator.getQueryExecution(queryId).hasNextResult()) {
          // The query will be transited to FINISHED when invoking getBatchResult() at the last time
          // So we don't need to clean up it manually
          Optional<TsBlock> tsBlock;
          try {
            tsBlock = coordinator.getQueryExecution(queryId).getBatchResult();
          } catch (IoTDBException e) {
            t = e;
            throw new QuerySchemaFetchFailedException(
                String.format("Fetch Schema failed: %s", e.getMessage()), e.getErrorCode());
          }
          if (!tsBlock.isPresent() || tsBlock.get().isEmpty()) {
            break;
          }
          Column column = tsBlock.get().getColumn(0);
          for (int i = 0; i < column.getPositionCount(); i++) {
            parseFetchedData(column.getBinary(i), result, deserializer, databaseSet, context);
          }
        }
        result.setDatabases(databaseSet);
        return result;
      }
    } catch (Throwable throwable) {
      t = throwable;
      throw throwable;
    } finally {
      coordinator.cleanupQueryExecution(queryId, null, t);
    }
  }

  private void parseFetchedData(
      Binary data,
      ClusterSchemaTree resultSchemaTree,
      ClusterSchemaTree.SchemaNodeBatchDeserializer deserializer,
      Set<String> databaseSet,
      MPPQueryContext context) {
    InputStream inputStream = new ByteArrayInputStream(data.getValues());
    try {
      byte type = ReadWriteIOUtils.readByte(inputStream);
      if (type == 0) {
        int size = ReadWriteIOUtils.readInt(inputStream);
        for (int i = 0; i < size; i++) {
          databaseSet.add(ReadWriteIOUtils.readString(inputStream));
        }
      } else if (type == 1) {
        // for data from old version
        ClusterSchemaTree deserializedSchemaTree = ClusterSchemaTree.deserialize(inputStream);
        if (context != null) {
          context.reserveMemoryForSchemaTree(deserializedSchemaTree.ramBytesUsed());
        }
        resultSchemaTree.mergeSchemaTree(deserializedSchemaTree);
      } else if (type == 2 || type == 3) {
        if (deserializer.isFirstBatch()) {
          long memCost = ReadWriteIOUtils.readLong(inputStream);
          if (context != null) {
            context.reserveMemoryForSchemaTree(memCost);
          }
        }
        deserializer.deserializeFromBatch(inputStream);
        if (type == 3) {
          // 'type == 3' indicates this batch is finished
          resultSchemaTree.mergeSchemaTree(deserializer.finish());
        }
      } else {
        throw new RuntimeException(
            new MetadataException("Failed to fetch schema because of unrecognized data"));
      }
    } catch (MemoryNotEnoughException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
