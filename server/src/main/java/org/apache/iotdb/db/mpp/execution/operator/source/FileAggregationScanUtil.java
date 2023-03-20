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

package org.apache.iotdb.db.mpp.execution.operator.source;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.aggregation.AccumulatorFactory;
import org.apache.iotdb.db.mpp.aggregation.Aggregator;
import org.apache.iotdb.db.mpp.plan.analyze.GroupByLevelController;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.AggregationDescriptor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.reader.materializer.TsFileResourceMaterializer;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPageReader;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileAggregationScanUtil {

  private final PartialPath pathPattern;

  private final AggregationDescriptor aggregationDescriptor;

  private final Map<PartialPath, Aggregator> pathToAggregatorMap;

  private TsFileResourceMaterializer fileResourceMaterializer;

  private final Map<PartialPath, List<IChunkMetadata>> chunkMetadataMap;

  private final PartialPathPool partialPathPool;

  private final int[] levels;

  private final SeriesScanOptions scanOptions;

  public FileAggregationScanUtil(
      PartialPath pathPattern,
      AggregationDescriptor aggregationDescriptor,
      int[] levels,
      SeriesScanOptions scanOptions) {
    this.pathPattern = pathPattern;
    this.aggregationDescriptor = aggregationDescriptor;
    this.pathToAggregatorMap = new HashMap<>();
    this.chunkMetadataMap = new HashMap<>();
    this.partialPathPool = new PartialPathPool();
    this.levels = levels;
    this.scanOptions = scanOptions;
  }

  public void initQueryDataSource(QueryDataSource dataSource) {
    this.fileResourceMaterializer = new TsFileResourceMaterializer(dataSource);
  }

  public boolean hasNextFile() {
    return fileResourceMaterializer.hasNext();
  }

  public void consume() throws IOException {
    pathToAggregatorMap.clear();

    TsFileResource nextFile = fileResourceMaterializer.next();
    TsFileSequenceReader reader =
        FileReaderManager.getInstance().get(nextFile.getTsFilePath(), nextFile.isClosed());
    List<String> allDevices = reader.getAllDevices();
    for (String device : allDevices) {
      PartialPath devicePath = partialPathPool.get(device);
      List<TimeseriesMetadata> timeseriesMetadataMap = reader.getDeviceTimeseriesMetadata(device);
      for (TimeseriesMetadata timeseriesMetadata : timeseriesMetadataMap) {
        consumeTimeseriesMetadata(devicePath, timeseriesMetadata);
      }
    }

    for (Map.Entry<PartialPath, List<IChunkMetadata>> entry : chunkMetadataMap.entrySet()) {
      PartialPath device = entry.getKey();
      for (IChunkMetadata chunkMetadata : entry.getValue()) {
        unpackChunkMetadata(device, chunkMetadata);
      }
    }
  }

  public TsBlock getAggregationResult(TsBlockBuilder builder) {
    for (Map.Entry<PartialPath, Aggregator> entry : pathToAggregatorMap.entrySet()) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getValueColumnBuilders()[0].writeBinary(
          Binary.valueOf(
              String.format(
                  "%s(%s)",
                  aggregationDescriptor.getAggregationFuncName(), entry.getKey().toString())));

      Aggregator aggregator = entry.getValue();
      ColumnBuilder[] columnBuilders = new ColumnBuilder[1];
      columnBuilders[0] = builder.getValueColumnBuilders()[1];
      aggregator.outputResult(columnBuilders);

      builder.getValueColumnBuilders()[2].writeBinary(
          Binary.valueOf(aggregator.getOutputType()[0].toString()));

      builder.declarePosition();
    }
    return builder.build();
  }

  private void unpackChunkMetadata(PartialPath devicePath, IChunkMetadata chunkMetadata)
      throws IOException {
    PartialPath groupedPath =
        partialPathPool.getGroupedPath(devicePath, chunkMetadata.getMeasurementUid());

    List<IPageReader> pageReaderList =
        FileLoaderUtils.loadPageReaderList(chunkMetadata, scanOptions.getGlobalTimeFilter());
    for (IPageReader pageReader : pageReaderList) {
      Filter queryFilter = scanOptions.getQueryFilter();
      Statistics statistics = pageReader.getStatistics();

      if (queryFilter == null || queryFilter.allSatisfy(statistics)) {
        updateAggregationResult(groupedPath, statistics);
      } else if (queryFilter.allNotSatisfy(statistics)) {
        // skip
      } else {
        pageReader.setFilter(scanOptions.getQueryFilter());
        updateAggregationResult(groupedPath, pageReader.getAllSatisfiedData());
      }
    }
  }

  private void consumeTimeseriesMetadata(
      PartialPath devicePath, TimeseriesMetadata timeseriesMetadata) {
    PartialPath fullPath = devicePath.concatNode(timeseriesMetadata.getMeasurementId());
    if (!pathPattern.matchFullPath(fullPath)) {
      return;
    }

    PartialPath groupedPath =
        partialPathPool.getGroupedPath(devicePath, timeseriesMetadata.getMeasurementId());
    if (pathToAggregatorMap.containsKey(groupedPath)) {
      pathToAggregatorMap.put(
          groupedPath,
          new Aggregator(
              AccumulatorFactory.createAccumulator(
                  TAggregationType.valueOf(
                      aggregationDescriptor.getAggregationFuncName().toUpperCase()),
                  timeseriesMetadata.getTSDataType(),
                  Collections.emptyList(),
                  Collections.emptyMap(),
                  true),
              aggregationDescriptor.getStep()));
    }

    Filter queryFilter = scanOptions.getQueryFilter();
    Statistics statistics = timeseriesMetadata.getStatistics();

    if (queryFilter == null || queryFilter.allSatisfy(statistics)) {
      updateAggregationResult(groupedPath, statistics);
    } else if (queryFilter.allNotSatisfy(statistics)) {
      // skip
    } else {
      consumeChunkMetadataList(devicePath, groupedPath, timeseriesMetadata.getChunkMetadataList());
    }
  }

  private void consumeChunkMetadataList(
      PartialPath devicePath, PartialPath groupedPath, List<IChunkMetadata> chunkMetadataList) {
    for (IChunkMetadata chunkMetadata : chunkMetadataList) {
      Filter queryFilter = scanOptions.getQueryFilter();
      Statistics statistics = chunkMetadata.getStatistics();

      if (queryFilter == null || queryFilter.allSatisfy(statistics)) {
        updateAggregationResult(groupedPath, statistics);
      } else if (queryFilter.allNotSatisfy(statistics)) {
        // skip
      } else {
        chunkMetadataMap.computeIfAbsent(devicePath, key -> new ArrayList<>()).add(chunkMetadata);
      }
    }
  }

  private void updateAggregationResult(PartialPath groupedPath, Statistics statistics) {
    Statistics[] statisticsList = new Statistics[1];
    statisticsList[0] = statistics;
    pathToAggregatorMap.get(groupedPath).processStatistics(statisticsList);
  }

  private void updateAggregationResult(PartialPath groupedPath, TsBlock tsBlock) {
    pathToAggregatorMap.get(groupedPath).processTsBlock(tsBlock, null, tsBlock.getPositionCount());
  }

  private class PartialPathPool {
    Map<String, PartialPath> pool;
    Map<String, PartialPath> rawPathToGroupedPathMap;

    public PartialPathPool() {
      this.pool = new HashMap<>();
      this.rawPathToGroupedPathMap = new HashMap<>();
    }

    public PartialPath get(String pathStr) {
      if (pool.containsKey(pathStr)) {
        return pool.get(pathStr);
      } else {
        PartialPath path = null;
        try {
          path = new PartialPath(pathStr);
        } catch (IllegalPathException ignored) {

        }
        pool.put(pathStr, path);
        return path;
      }
    }

    public PartialPath getGroupedPath(PartialPath devicePath, String measurementId) {
      String rawPathStr = devicePath.getDevice().concat(measurementId);
      if (rawPathToGroupedPathMap.containsKey(rawPathStr)) {
        return rawPathToGroupedPathMap.get(rawPathStr);
      }
      PartialPath groupedPath =
          GroupByLevelController.groupPathByLevel(devicePath, measurementId, levels);
      rawPathToGroupedPathMap.put(rawPathStr, groupedPath);
      return groupedPath;
    }
  }
}
