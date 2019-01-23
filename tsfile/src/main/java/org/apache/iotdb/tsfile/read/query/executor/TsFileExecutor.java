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
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.ChunkLoader;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.query.dataset.DataSetWithoutTimeGenerator;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
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
      } catch (QueryFilterOptimizationException e) {
        throw new IOException(e);
      }
    } else {
      return execute(queryExpression.getSelectedSeries());
    }
  }

  /**
   * no filter, can use multi-way merge.
   *
   * @param selectedPathList all selected paths
   * @return DataSet without TimeGenerator
   */
  private QueryDataSet execute(List<Path> selectedPathList) throws IOException {
    List<FileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    for (Path path : selectedPathList) {
      List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
      FileSeriesReader seriesReader = new FileSeriesReaderWithoutFilter(chunkLoader,
          chunkMetaDataList);
      readersOfSelectedSeries.add(seriesReader);
      dataTypes.add(chunkMetaDataList.get(0).getTsDataType());
    }
    return new DataSetWithoutTimeGenerator(selectedPathList, dataTypes, readersOfSelectedSeries);
  }

  /**
   * has a GlobalTimeExpression, can use multi-way merge.
   *
   * @param selectedPathList all selected paths
   * @param timeFilter GlobalTimeExpression that takes effect to all selected paths
   * @return DataSet without TimeGenerator
   */
  private QueryDataSet execute(List<Path> selectedPathList, GlobalTimeExpression timeFilter)
      throws IOException {
    List<FileSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    for (Path path : selectedPathList) {
      List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
      FileSeriesReader seriesReader = new FileSeriesReaderWithFilter(chunkLoader, chunkMetaDataList,
          timeFilter.getFilter());
      readersOfSelectedSeries.add(seriesReader);
      dataTypes.add(chunkMetaDataList.get(0).getTsDataType());
    }

    return new DataSetWithoutTimeGenerator(selectedPathList, dataTypes, readersOfSelectedSeries);
  }

}
