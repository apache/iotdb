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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.QueryContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.AlignedDescPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.AlignedPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.DescPriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.common.PriorityMergeReader;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IPointReader;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AlignedSeriesScanUtil extends SeriesScanUtil {

  private final List<TSDataType> dataTypes;

  // only used for limit and offset push down optimizer, if we select all columns from aligned
  // device, we
  // can use statistics to skip.
  // it's only exact while using limit & offset push down
  private final boolean queryAllSensors;

  public AlignedSeriesScanUtil(
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context) {
    this(seriesPath, scanOrder, scanOptions, context, false, null);
  }

  public AlignedSeriesScanUtil(
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context,
      boolean queryAllSensors,
      List<TSDataType> givenDataTypes) {
    super(seriesPath, scanOrder, scanOptions, context);
    dataTypes =
        givenDataTypes != null
            ? givenDataTypes
            : ((AlignedPath) seriesPath)
                .getSchemaList().stream()
                    .map(IMeasurementSchema::getType)
                    .collect(Collectors.toList());
    isAligned = true;
    this.queryAllSensors = queryAllSensors;
  }

  @SuppressWarnings("squid:S3740")
  @Override
  protected Statistics currentFileStatistics(int index) {
    return ((AlignedTimeSeriesMetadata) firstTimeSeriesMetadata)
        .getMeasurementStatistics(index)
        .orElse(null);
  }

  @SuppressWarnings("squid:S3740")
  @Override
  protected Statistics currentFileTimeStatistics() {
    return ((AlignedTimeSeriesMetadata) firstTimeSeriesMetadata).getTimeStatistics();
  }

  @SuppressWarnings("squid:S3740")
  @Override
  protected Statistics currentChunkStatistics(int index) {
    return ((AlignedChunkMetadata) firstChunkMetadata).getMeasurementStatistics(index).orElse(null);
  }

  @SuppressWarnings("squid:S3740")
  @Override
  protected Statistics currentChunkTimeStatistics() {
    return ((AlignedChunkMetadata) firstChunkMetadata).getTimeStatistics();
  }

  @SuppressWarnings("squid:S3740")
  @Override
  protected Statistics currentPageStatistics(int index) throws IOException {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.getStatistics(index);
  }

  @SuppressWarnings("squid:S3740")
  @Override
  protected Statistics currentPageTimeStatistics() throws IOException {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.getTimeStatistics();
  }

  @Override
  protected PriorityMergeReader getPriorityMergeReader() {
    return new AlignedPriorityMergeReader();
  }

  @Override
  protected DescPriorityMergeReader getDescPriorityMergeReader() {
    return new AlignedDescPriorityMergeReader();
  }

  @Override
  protected AlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource,
      PartialPath seriesPath,
      QueryContext context,
      Filter filter,
      Set<String> allSensors)
      throws IOException {
    return FileLoaderUtils.loadTimeSeriesMetadata(
        resource, (AlignedPath) seriesPath, context, filter, queryAllSensors);
  }

  @Override
  protected List<TSDataType> getTsDataTypeList() {
    return dataTypes;
  }

  @Override
  protected IPointReader getPointReader(TsBlock tsBlock) {
    return tsBlock.getTsBlockAlignedRowIterator();
  }

  @Override
  protected void filterFirstTimeSeriesMetadata() throws IOException {
    if (firstTimeSeriesMetadata != null
        && !isFileOverlapped()
        && !firstTimeSeriesMetadata.isModified()) {
      Filter queryFilter = scanOptions.getPushDownFilter();
      Statistics statistics = firstTimeSeriesMetadata.getStatistics();
      if (queryFilter == null || queryFilter.allSatisfy(statistics)) {
        skipOffsetByTimeSeriesMetadata();
      } else if (queryFilter.canSkip(statistics)) {
        skipCurrentFile();
      }
    }
  }

  private void skipOffsetByTimeSeriesMetadata() {
    if (!canSkipOffsetByTimeSeriesMetadata((AlignedTimeSeriesMetadata) firstTimeSeriesMetadata)) {
      return;
    }

    long rowCount = firstTimeSeriesMetadata.getTimeStatistics().getCount();
    if (paginationController.hasCurOffset(rowCount)) {
      skipCurrentFile();
      paginationController.consumeOffset(rowCount);
    }
  }

  private boolean canSkipOffsetByTimeSeriesMetadata(
      AlignedTimeSeriesMetadata alignedTimeSeriesMetadata) {
    if (queryAllSensors || alignedTimeSeriesMetadata.getMeasurementCount() == 0) {
      return true;
    }

    // For aligned series, we can use statistics to skip OFFSET only when all times are selected.
    // NOTE: if we change the query semantic in the future for aligned series, we need to remove
    // this check here.
    return alignedTimeSeriesMetadata.timeAllSelected();
  }

  @Override
  protected void filterFirstChunkMetadata() throws IOException {
    if (firstChunkMetadata != null && !isChunkOverlapped() && !firstChunkMetadata.isModified()) {
      Filter queryFilter = scanOptions.getPushDownFilter();
      Statistics statistics = firstChunkMetadata.getStatistics();
      if (queryFilter == null || queryFilter.allSatisfy(statistics)) {
        skipOffsetByChunkMetadata();
      } else if (queryFilter.canSkip(statistics)) {
        skipCurrentChunk();
      }
    }
  }

  private void skipOffsetByChunkMetadata() {
    if (!canSkipOffsetByChunkMetadata((AlignedChunkMetadata) firstChunkMetadata)) {
      return;
    }

    long rowCount = firstChunkMetadata.getStatistics().getCount();
    if (paginationController.hasCurOffset(rowCount)) {
      skipCurrentChunk();
      paginationController.consumeOffset(rowCount);
    }
  }

  private boolean canSkipOffsetByChunkMetadata(AlignedChunkMetadata alignedChunkMetadata) {
    if (queryAllSensors || alignedChunkMetadata.getMeasurementCount() == 0) {
      return true;
    }

    // For aligned series, we can use statistics to skip OFFSET only when all times are selected.
    // NOTE: if we change the query semantic in the future for aligned series, we need to remove
    // this check here.
    return alignedChunkMetadata.timeAllSelected();
  }
}
