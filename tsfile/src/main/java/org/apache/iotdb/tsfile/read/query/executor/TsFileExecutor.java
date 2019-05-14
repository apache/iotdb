/**
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
package org.apache.iotdb.tsfile.read.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.tsfile.common.constant.QueryConstant;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier.LoadMode;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.query.dataset.DataSetWithoutTimeGenerator;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.series.EmptyFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithFilter;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderWithoutFilter;

public class TsFileExecutor implements QueryExecutor {

  private MetadataQuerier metadataQuerier;
  private ChunkLoader chunkLoader;

  public TsFileExecutor(MetadataQuerier metadataQuerier, ChunkLoader chunkLoader) {
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;
  }

  @Override
  public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
    metadataQuerier.loadChunkMetaDatas(queryExpression.getSelectedSeries());
    if (queryExpression.hasQueryFilter()) {
      try {
        IExpression expression = queryExpression.getExpression();
        IExpression regularIExpression = ExpressionOptimizer.getInstance().optimize(expression,
            queryExpression.getSelectedSeries());
        queryExpression.setExpression(regularIExpression);

        if (regularIExpression instanceof GlobalTimeExpression) {
          return execute(queryExpression.getSelectedSeries(),
              (GlobalTimeExpression) regularIExpression);
        } else {
          return new ExecutorWithTimeGenerator(metadataQuerier, chunkLoader)
              .execute(queryExpression);
        }
      } catch (QueryFilterOptimizationException | NoMeasurementException e) {
        throw new IOException(e);
      }
    } else {
      try {
        return execute(queryExpression.getSelectedSeries());
      } catch (NoMeasurementException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   *
   * @param queryExpression
   * @param params
   * @return
   * @throws IOException
   */
  public QueryDataSet execute(QueryExpression queryExpression, HashMap<String, Long> params) throws IOException {
    if (!params.containsKey(QueryConstant.PARTITION_START_OFFSET)
        || !params.containsKey(QueryConstant.PARTITION_END_OFFSET)) {
      throw new IllegalArgumentException(
          "Current supported query parameters are the pair of [partition_start_offset:long, partition_end_offset:long].");
    }
    long partitionStartOffset = params.get(QueryConstant.PARTITION_START_OFFSET);
    long partitionEndOffset = params.get(QueryConstant.PARTITION_END_OFFSET);

    // (1) get the sorted union covered time ranges of chunkGroups in the current partition
    ArrayList<TimeRange> timeRangesIn = metadataQuerier
        .getTimeRangeInOrPrev(queryExpression.getSelectedSeries(), LoadMode.InPartition,
            partitionStartOffset, partitionEndOffset);

    // (2) check if null
    if (timeRangesIn.size() == 0) {
      return new DataSetWithoutTimeGenerator(new ArrayList<>(), new ArrayList<>(),
          new ArrayList<>()); // return empty QueryDataSet
    }

    // (3) get the sorted union covered time ranges of chunkGroups before the current partition
    ArrayList<TimeRange> timeRangesPrev = metadataQuerier
        .getTimeRangeInOrPrev(queryExpression.getSelectedSeries(), LoadMode.PrevPartition,
            partitionStartOffset, partitionEndOffset);

    // (4) calculate the remaining time ranges
    ArrayList<TimeRange> timeRangesRemains = new ArrayList<>();
    for (TimeRange in : timeRangesIn) {
      ArrayList<TimeRange> remains = new ArrayList<>(in.getRemains(timeRangesPrev));
      timeRangesRemains.addAll(remains);
    }

    // (5) check if null
    if (timeRangesRemains.size() == 0) {
      return new DataSetWithoutTimeGenerator(new ArrayList<>(), new ArrayList<>(),
          new ArrayList<>()); // return empty QueryDataSet
    }

    // (6) construct an additional time filter based on the remaining time ranges
    IExpression addTimeExpression = timeRangesRemains.get(0).getExpression();
    for (int i = 1; i < timeRangesRemains.size(); i++) {
      addTimeExpression = BinaryExpression
          .or(addTimeExpression, timeRangesRemains.get(i).getExpression());
    }

    // (7) combine the original query expression and the additional time filter
    if (queryExpression.hasQueryFilter()) {
      IExpression combinedExpression = BinaryExpression
          .and(queryExpression.getExpression(), addTimeExpression);
      queryExpression.setExpression(combinedExpression);
    } else {
      queryExpression.setExpression(addTimeExpression);
    }

    // (8) Having converted the query partition constraint to an additional time filter, we can now query as normal.
    return execute(queryExpression);
  }

  /**
   * no filter, can use multi-way merge.
   *
   * @param selectedPathList all selected paths
   * @return DataSet without TimeGenerator
   */
  private QueryDataSet execute(List<Path> selectedPathList)
      throws IOException, NoMeasurementException {
    return executeMayAttachTimeFiler(selectedPathList, null);
  }

  /**
   * has a GlobalTimeExpression, can use multi-way merge.
   *
   * @param selectedPathList all selected paths
   * @param timeFilter GlobalTimeExpression that takes effect to all selected paths
   * @return DataSet without TimeGenerator
   */
  private QueryDataSet execute(List<Path> selectedPathList, GlobalTimeExpression timeFilter)
      throws IOException, NoMeasurementException {
    return executeMayAttachTimeFiler(selectedPathList, timeFilter);
  }

  /**
   * @param selectedPathList completed path
   * @param timeFilter a GlobalTimeExpression or null
   * @return DataSetWithoutTimeGenerator
   */
  private QueryDataSet executeMayAttachTimeFiler(List<Path> selectedPathList,
      GlobalTimeExpression timeFilter)
      throws IOException, NoMeasurementException {
    List<FileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    for (Path path : selectedPathList) {
      List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
      FileSeriesReader seriesReader;
      if (chunkMetaDataList.isEmpty()) {
        seriesReader = new EmptyFileSeriesReader();
        dataTypes.add(metadataQuerier.getDataType(path.getMeasurement()));
      } else {
        if (timeFilter == null) {
          seriesReader = new FileSeriesReaderWithoutFilter(chunkLoader, chunkMetaDataList);
        } else {
          seriesReader = new FileSeriesReaderWithFilter(chunkLoader, chunkMetaDataList,
              timeFilter.getFilter());
        }
        dataTypes.add(chunkMetaDataList.get(0).getTsDataType());
      }
      readersOfSelectedSeries.add(seriesReader);
    }
    return new DataSetWithoutTimeGenerator(selectedPathList, dataTypes, readersOfSelectedSeries);
  }

}
