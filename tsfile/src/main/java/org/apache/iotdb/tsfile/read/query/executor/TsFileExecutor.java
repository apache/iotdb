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
import java.util.List;
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
    LoadMode mode = metadataQuerier.getLoadMode();
    if (mode == LoadMode.PrevPartition) {
      throw new IOException("Wrong use of PrevPartition mode.");
    }

    if (metadataQuerier.getLoadMode() == LoadMode.InPartition) {
      // (1) get the sorted union covered time ranges of chunkGroups in the current partition
      ArrayList<TimeRange> timeRangesIn = metadataQuerier
          .getTimeRangeInOrPrev(queryExpression.getSelectedSeries(), LoadMode.InPartition);

      // (2) check if null
      if (timeRangesIn.size() == 0) {
        return new DataSetWithoutTimeGenerator(new ArrayList<Path>(), new ArrayList<TSDataType>(),
            new ArrayList<FileSeriesReader>()); // return empty QueryDataSet
      }

      // (3) get the sorted union covered time ranges of chunkGroups before the current partition
      ArrayList<TimeRange> timeRangesPrev = metadataQuerier
          .getTimeRangeInOrPrev(queryExpression.getSelectedSeries(), LoadMode.PrevPartition);

      // (4) calculate the remaining time range
      ArrayList<TimeRange> timeRangesRemains = new ArrayList<>();
      for (TimeRange in : timeRangesIn) {
        ArrayList<TimeRange> remains = new ArrayList<>(in.getRemains(timeRangesPrev));
        timeRangesRemains.addAll(remains);
      }

      // (5) check if null
      if (timeRangesRemains.size() == 0) {
        return new DataSetWithoutTimeGenerator(new ArrayList<Path>(), new ArrayList<TSDataType>(),
            new ArrayList<FileSeriesReader>()); // return empty QueryDataSet
      }

      // (6) add an additional global time filter based on the remaining time range
      IExpression timeBound = timeRangesRemains.get(0).getExpression();
      for (int i = 1; i < timeRangesRemains.size(); i++) {
        timeBound = BinaryExpression
            .or(timeBound, timeRangesRemains.get(i).getExpression());
      }

      if (queryExpression.hasQueryFilter()) {
        IExpression timeBoundExpression = BinaryExpression
            .and(queryExpression.getExpression(), timeBound);
        queryExpression.setExpression(timeBoundExpression);
      } else {
        queryExpression.setExpression(timeBound);
      }

      // (7) with global time filters, we can now remove partition constraints
      metadataQuerier.setLoadMode(LoadMode.NoPartition);
      metadataQuerier.loadChunkMetaDatas(queryExpression.getSelectedSeries());

    } else { // NoPartition mode
      metadataQuerier.loadChunkMetaDatas(queryExpression.getSelectedSeries());
    }

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
