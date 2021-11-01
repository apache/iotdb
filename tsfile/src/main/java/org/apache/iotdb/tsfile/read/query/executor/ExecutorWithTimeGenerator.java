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

import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.query.dataset.DataSetWithTimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.query.timegenerator.TsFileTimeGenerator;
import org.apache.iotdb.tsfile.read.reader.series.FileSeriesReaderByTimestamp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

public class ExecutorWithTimeGenerator implements QueryExecutor {

  private IMetadataQuerier metadataQuerier;
  private IChunkLoader chunkLoader;

  public ExecutorWithTimeGenerator(IMetadataQuerier metadataQuerier, IChunkLoader chunkLoader) {
    this.metadataQuerier = metadataQuerier;
    this.chunkLoader = chunkLoader;
  }

  /**
   * All leaf nodes of queryFilter in queryExpression are SeriesFilters, We use a TimeGenerator to
   * control query processing. for more information, see DataSetWithTimeGenerator
   *
   * @return DataSet with TimeGenerator
   */
  @Override
  public DataSetWithTimeGenerator execute(QueryExpression queryExpression) throws IOException {

    IExpression expression = queryExpression.getExpression();
    List<Path> selectedPathList = queryExpression.getSelectedSeries();

    // get TimeGenerator by IExpression
    TimeGenerator timeGenerator = new TsFileTimeGenerator(expression, chunkLoader, metadataQuerier);

    // the size of hasFilter is equal to selectedPathList, if a series has a filter, it is true,
    // otherwise false
    List<Boolean> cached =
        markFilterdPaths(expression, selectedPathList, timeGenerator.hasOrNode());
    List<FileSeriesReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();

    Iterator<Boolean> cachedIterator = cached.iterator();
    Iterator<Path> selectedPathIterator = selectedPathList.iterator();
    while (cachedIterator.hasNext()) {
      boolean cachedValue = cachedIterator.next();
      Path selectedPath = selectedPathIterator.next();

      List<IChunkMetadata> chunkMetadataList = metadataQuerier.getChunkMetaDataList(selectedPath);
      if (chunkMetadataList.size() != 0) {
        dataTypes.add(chunkMetadataList.get(0).getDataType());
        if (cachedValue) {
          readersOfSelectedSeries.add(null);
          continue;
        }
        FileSeriesReaderByTimestamp seriesReader =
            new FileSeriesReaderByTimestamp(chunkLoader, chunkMetadataList);
        readersOfSelectedSeries.add(seriesReader);
      } else {
        selectedPathIterator.remove();
        cachedIterator.remove();
      }
    }

    return new DataSetWithTimeGenerator(
        selectedPathList, cached, dataTypes, timeGenerator, readersOfSelectedSeries);
  }

  public static List<Boolean> markFilterdPaths(
      IExpression expression, List<Path> selectedPaths, boolean hasOrNode) {
    List<Boolean> cached = new ArrayList<>();
    if (hasOrNode) {
      for (Path ignored : selectedPaths) {
        cached.add(false);
      }
      return cached;
    }

    HashSet<Path> filteredPaths = new HashSet<>();
    getAllFilteredPaths(expression, filteredPaths);

    for (Path selectedPath : selectedPaths) {
      cached.add(filteredPaths.contains(selectedPath));
    }

    return cached;
  }

  private static void getAllFilteredPaths(IExpression expression, HashSet<Path> paths) {
    if (expression instanceof BinaryExpression) {
      getAllFilteredPaths(((BinaryExpression) expression).getLeft(), paths);
      getAllFilteredPaths(((BinaryExpression) expression).getRight(), paths);
    } else if (expression instanceof SingleSeriesExpression) {
      paths.add(((SingleSeriesExpression) expression).getSeriesPath());
    }
  }
}
