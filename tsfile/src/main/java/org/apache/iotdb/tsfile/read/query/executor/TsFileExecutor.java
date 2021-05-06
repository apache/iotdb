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
package org.apache.iotdb.tsfile.read.query.executor;

import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.NoMeasurementException;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.query.dataset.DataSetWithoutTimeGenerator;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.reader.series.AbstractFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.EmptyFileSeriesReader;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReader;
import org.apache.iotdb.tsfile.utils.BloomFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TsFileExecutor implements QueryExecutor {

  private IMetadataQuerier metadataQuerier;
  private IChunkLoader chunkLoader;

  public TsFileExecutor(IMetadataQuerier metadataQuerier, IChunkLoader chunkLoader) {
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;
  }

  @Override
  public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
    // bloom filter
    BloomFilter bloomFilter = metadataQuerier.getWholeFileMetadata().getBloomFilter();
    List<Path> filteredSeriesPath = new ArrayList<>();
    if (bloomFilter != null) {
      for (Path path : queryExpression.getSelectedSeries()) {
        if (bloomFilter.contains(path.getFullPath())) {
          filteredSeriesPath.add(path);
        }
      }
      queryExpression.setSelectSeries(filteredSeriesPath);
    }

    metadataQuerier.loadChunkMetaDatas(queryExpression.getSelectedSeries());
    if (queryExpression.hasQueryFilter()) {
      try {
        IExpression expression = queryExpression.getExpression();
        IExpression regularIExpression =
            ExpressionOptimizer.getInstance()
                .optimize(expression, queryExpression.getSelectedSeries());
        queryExpression.setExpression(regularIExpression);

        if (regularIExpression instanceof GlobalTimeExpression) {
          return execute(
              queryExpression.getSelectedSeries(), (GlobalTimeExpression) regularIExpression);
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
   * Query with the space partition constraint.
   *
   * @param queryExpression query expression
   * @param spacePartitionStartPos the start position of the space partition
   * @param spacePartitionEndPos the end position of the space partition
   * @return QueryDataSet
   */
  public QueryDataSet execute(
      QueryExpression queryExpression, long spacePartitionStartPos, long spacePartitionEndPos)
      throws IOException {
    // convert the space partition constraint to the time partition constraint
    ArrayList<TimeRange> resTimeRanges =
        new ArrayList<>(
            metadataQuerier.convertSpace2TimePartition(
                queryExpression.getSelectedSeries(), spacePartitionStartPos, spacePartitionEndPos));

    // check if resTimeRanges is empty
    if (resTimeRanges.isEmpty()) {
      return new DataSetWithoutTimeGenerator(
          Collections.emptyList(),
          Collections.emptyList(),
          Collections.emptyList()); // return an empty QueryDataSet
    }

    // construct an additional time filter based on the time partition constraint
    IExpression addTimeExpression = resTimeRanges.get(0).getExpression();
    for (int i = 1; i < resTimeRanges.size(); i++) {
      addTimeExpression =
          BinaryExpression.or(addTimeExpression, resTimeRanges.get(i).getExpression());
    }

    // combine the original query expression and the additional time filter
    if (queryExpression.hasQueryFilter()) {
      IExpression combinedExpression =
          BinaryExpression.and(queryExpression.getExpression(), addTimeExpression);
      queryExpression.setExpression(combinedExpression);
    } else {
      queryExpression.setExpression(addTimeExpression);
    }

    // Having converted the space partition constraint to an additional time filter, we can now
    // query as normal.
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
   * @param timeExpression a GlobalTimeExpression or null
   * @return DataSetWithoutTimeGenerator
   */
  private QueryDataSet executeMayAttachTimeFiler(
      List<Path> selectedPathList, GlobalTimeExpression timeExpression)
      throws IOException, NoMeasurementException {
    List<AbstractFileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    for (Path path : selectedPathList) {
      List<IChunkMetadata> chunkMetadataList = metadataQuerier.getChunkMetaDataList(path);
      AbstractFileSeriesReader seriesReader;
      if (chunkMetadataList.isEmpty()) {
        seriesReader = new EmptyFileSeriesReader();
        dataTypes.add(metadataQuerier.getDataType(path));
      } else {
        if (timeExpression == null) {
          seriesReader = new FileSeriesReader(chunkLoader, chunkMetadataList, null);
        } else {
          seriesReader =
              new FileSeriesReader(chunkLoader, chunkMetadataList, timeExpression.getFilter());
        }
        dataTypes.add(chunkMetadataList.get(0).getDataType());
      }
      readersOfSelectedSeries.add(seriesReader);
    }
    return new DataSetWithoutTimeGenerator(selectedPathList, dataTypes, readersOfSelectedSeries);
  }
}
