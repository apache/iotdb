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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.universal.AlignedDescPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.AlignedPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.DescPriorityMergeReader;
import org.apache.iotdb.db.query.reader.universal.PriorityMergeReader;
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

  public AlignedSeriesScanUtil(
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions scanOptions,
      FragmentInstanceContext context) {
    super(seriesPath, scanOrder, scanOptions, context);
    dataTypes =
        ((AlignedPath) seriesPath)
            .getSchemaList().stream().map(IMeasurementSchema::getType).collect(Collectors.toList());
    isAligned = true;
  }

  @Override
  protected Statistics currentFileStatistics(int index) throws IOException {
    return ((AlignedTimeSeriesMetadata) firstTimeSeriesMetadata).getStatistics(index);
  }

  @Override
  protected Statistics currentFileTimeStatistics() throws IOException {
    return ((AlignedTimeSeriesMetadata) firstTimeSeriesMetadata).getTimeStatistics();
  }

  @Override
  protected Statistics currentChunkStatistics(int index) throws IOException {
    return ((AlignedChunkMetadata) firstChunkMetadata).getStatistics(index);
  }

  @Override
  protected Statistics currentChunkTimeStatistics() {
    return ((AlignedChunkMetadata) firstChunkMetadata).getTimeStatistics();
  }

  @Override
  protected Statistics currentPageStatistics(int index) throws IOException {
    if (firstPageReader == null) {
      return null;
    }
    return firstPageReader.getStatistics(index);
  }

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
        resource, (AlignedPath) seriesPath, context, filter);
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
      Filter queryFilter = scanOptions.getQueryFilter();
      if (queryFilter != null) {
        // TODO accept valueStatisticsList to filter
        if (!queryFilter.satisfy(firstTimeSeriesMetadata.getStatistics())) {
          skipCurrentFile();
        }
      } else {
        // For aligned series, When we only query some measurements under an aligned device, if the
        // values of these queried measurements at a timestamp are all null, the timestamp will not
        // be selected.
        // NOTE: if we change the query semantic in the future for aligned series, we need to remove
        // this check here.
        long rowCount =
            ((AlignedTimeSeriesMetadata) firstTimeSeriesMetadata).getTimeStatistics().getCount();
        for (Statistics statistics :
            ((AlignedTimeSeriesMetadata) firstTimeSeriesMetadata).getValueStatisticsList()) {
          if (statistics == null || statistics.hasNullValue(rowCount)) {
            return;
          }
        }
        // When the number of points in all value chunk groups is the same as that in the time chunk
        // group, it means that there is no null value, and all timestamps will be selected.
        if (paginationController.hasCurOffset(rowCount)) {
          skipCurrentFile();
          paginationController.consumeOffset(rowCount);
        }
      }
    }
  }

  @Override
  protected void filterFirstChunkMetadata() throws IOException {
    if (firstChunkMetadata != null && !isChunkOverlapped() && !firstChunkMetadata.isModified()) {
      Filter queryFilter = scanOptions.getQueryFilter();
      if (queryFilter != null) {
        // TODO accept valueStatisticsList to filter
        if (!queryFilter.satisfy(firstChunkMetadata.getStatistics())) {
          skipCurrentChunk();
        }
      } else {
        // For aligned series, When we only query some measurements under an aligned device, if the
        // values of these queried measurements at a timestamp are all null, the timestamp will not
        // be selected.
        // NOTE: if we change the query semantic in the future for aligned series, we need to remove
        // this check here.
        long rowCount = firstChunkMetadata.getStatistics().getCount();
        for (Statistics statistics :
            ((AlignedChunkMetadata) firstChunkMetadata).getValueStatisticsList()) {
          if (statistics == null || statistics.hasNullValue(rowCount)) {
            return;
          }
        }
        // When the number of points in all value chunks is the same as that in the time chunk, it
        // means that there is no null value, and all timestamps will be selected.
        if (paginationController.hasCurOffset(rowCount)) {
          skipCurrentChunk();
          paginationController.consumeOffset(rowCount);
        }
      }
    }
  }
}
