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
package org.apache.iotdb.db.query.executor;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithValueFilter;
import org.apache.iotdb.db.query.dataset.NewEngineDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.dataset.NonAlignEngineDataSet;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderByTimestamp;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithoutValueFilter;
import org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * IoTDB query executor.
 */
public class EngineExecutor implements DataQueryExecutor {

  private List<Path> deduplicatedPaths;
  private List<TSDataType> deduplicatedDataTypes;
  private IExpression optimizedExpression;

  public EngineExecutor(List<Path> deduplicatedPaths, List<TSDataType> deduplicatedDataTypes,
      IExpression optimizedExpression) {
    this.deduplicatedPaths = deduplicatedPaths;
    this.deduplicatedDataTypes = deduplicatedDataTypes;
    this.optimizedExpression = optimizedExpression;
  }

  public EngineExecutor(List<Path> deduplicatedPaths, List<TSDataType> deduplicatedDataTypes) {
    this.deduplicatedPaths = deduplicatedPaths;
    this.deduplicatedDataTypes = deduplicatedDataTypes;
  }

  protected ManagedSeriesReader getSeriesReaderWithoutValueFilter(Path path,
      TSDataType dataType, Filter timeFilter,
      QueryContext context, boolean pushdownUnseq, TsFileFilter fileFilter) throws IOException,
      StorageEngineException {
    return new SeriesReaderWithoutValueFilter(path, dataType, timeFilter, context, pushdownUnseq,
        fileFilter);
  }

  protected IReaderByTimestamp getReaderByTimestamp(Path path, QueryContext context)
      throws IOException, StorageEngineException {
    return new SeriesReaderByTimestamp(path, context);
  }


  /**
   * without filter or with global time filter.
   */
  @Override
  public QueryDataSet executeWithoutValueFilter(QueryContext context)
      throws StorageEngineException, IOException {

    Filter timeFilter = null;
    if (optimizedExpression != null) {
      timeFilter = ((GlobalTimeExpression) optimizedExpression).getFilter();
    }

    List<ManagedSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      Path path = deduplicatedPaths.get(i);
      TSDataType dataType = deduplicatedDataTypes.get(i);

      ManagedSeriesReader reader = getSeriesReaderWithoutValueFilter(path, dataType, timeFilter, context,
          true, null);
      readersOfSelectedSeries.add(reader);
    }

    try {
      return new NewEngineDataSetWithoutValueFilter(deduplicatedPaths, deduplicatedDataTypes,
          readersOfSelectedSeries);
    } catch (InterruptedException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }
  
  public QueryDataSet executeNonAlign(QueryContext context)
      throws StorageEngineException, IOException {

    Filter timeFilter = null;
    if (optimizedExpression != null) {
      timeFilter = ((GlobalTimeExpression) optimizedExpression).getFilter();
    }

    List<ManagedSeriesReader> readersOfSelectedSeries = new ArrayList<>();
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      Path path = deduplicatedPaths.get(i);
      TSDataType dataType = deduplicatedDataTypes.get(i);

      ManagedSeriesReader reader = new SeriesReaderWithoutValueFilter(path, dataType, timeFilter, context,
          true, null);
      readersOfSelectedSeries.add(reader);
    }

    return new NonAlignEngineDataSet(deduplicatedPaths, deduplicatedDataTypes,
        readersOfSelectedSeries);
  }

  /**
   * executeWithValueFilter query.
   *
   * @return QueryDataSet object
   * @throws StorageEngineException StorageEngineException
   */
  @Override
  public QueryDataSet executeWithValueFilter(QueryContext context)
      throws StorageEngineException, IOException {

    EngineTimeGenerator timestampGenerator = getTimeGenerator(
        optimizedExpression, context);

    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
    for (Path path : deduplicatedPaths) {
      IReaderByTimestamp seriesReaderByTimestamp = getReaderByTimestamp(path, context);
      readersOfSelectedSeries.add(seriesReaderByTimestamp);
    }
    return new EngineDataSetWithValueFilter(deduplicatedPaths, deduplicatedDataTypes,
        timestampGenerator, readersOfSelectedSeries);
  }

  protected EngineTimeGenerator getTimeGenerator(IExpression expression,
      QueryContext context) throws StorageEngineException {
    return new EngineTimeGenerator(expression, context);
  }
}
