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

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithValueFilter;
import org.apache.iotdb.db.query.dataset.NonAlignEngineDataSet;
import org.apache.iotdb.db.query.dataset.RawQueryDataSetWithoutValueFilter;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.seriesRelated.IRawDataReader;
import org.apache.iotdb.db.query.reader.seriesRelated.RawDataReaderWithoutValueFilter;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderByTimestamp;
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
public class RawDataQueryExecutor {

  private List<Path> deduplicatedPaths;
  private List<TSDataType> deduplicatedDataTypes;
  private IExpression optimizedExpression;

  public RawDataQueryExecutor(List<Path> deduplicatedPaths, List<TSDataType> deduplicatedDataTypes,
      IExpression optimizedExpression) {
    this.deduplicatedPaths = deduplicatedPaths;
    this.deduplicatedDataTypes = deduplicatedDataTypes;
    this.optimizedExpression = optimizedExpression;
  }

  public RawDataQueryExecutor(List<Path> deduplicatedPaths, List<TSDataType> deduplicatedDataTypes) {
    this.deduplicatedPaths = deduplicatedPaths;
    this.deduplicatedDataTypes = deduplicatedDataTypes;
  }

  /**
   * without filter or with global time filter.
   */
  public QueryDataSet executeWithoutValueFilter(QueryContext context) throws StorageEngineException {

    Filter timeFilter = null;
    if (optimizedExpression != null) {
      timeFilter = ((GlobalTimeExpression) optimizedExpression).getFilter();
    }

    List<IRawDataReader> readersOfSelectedSeries = new ArrayList<>();
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      Path path = deduplicatedPaths.get(i);
      TSDataType dataType = deduplicatedDataTypes.get(i);

      IRawDataReader reader = new RawDataReaderWithoutValueFilter(path, dataType, timeFilter,
          context,
          QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter));
      readersOfSelectedSeries.add(reader);
    }

    try {
      return new RawQueryDataSetWithoutValueFilter(deduplicatedPaths, deduplicatedDataTypes,
          readersOfSelectedSeries);
    } catch (InterruptedException e) {
      throw new StorageEngineException(e.getMessage());
    }
  }

  public QueryDataSet executeNonAlign(QueryContext context) throws StorageEngineException {

    Filter timeFilter = null;
    if (optimizedExpression != null) {
      timeFilter = ((GlobalTimeExpression) optimizedExpression).getFilter();
    }

    List<IRawDataReader> readersOfSelectedSeries = new ArrayList<>();
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      Path path = deduplicatedPaths.get(i);
      TSDataType dataType = deduplicatedDataTypes.get(i);

      IRawDataReader reader = new RawDataReaderWithoutValueFilter(path, dataType, timeFilter,
          context,
          QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter));
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
  public QueryDataSet executeWithValueFilter(QueryContext context) throws StorageEngineException {

    EngineTimeGenerator timestampGenerator = new EngineTimeGenerator(
        optimizedExpression, context);

    List<IReaderByTimestamp> readersOfSelectedSeries = new ArrayList<>();
    for (int i = 0; i < deduplicatedPaths.size(); i++) {
      Path path = deduplicatedPaths.get(i);
      SeriesReaderByTimestamp seriesReaderByTimestamp = new SeriesReaderByTimestamp(path,
          deduplicatedDataTypes.get(i), context,
          QueryResourceManager.getInstance().getQueryDataSource(path, context, null));
      readersOfSelectedSeries.add(seriesReaderByTimestamp);
    }
    return new EngineDataSetWithValueFilter(deduplicatedPaths, deduplicatedDataTypes,
        timestampGenerator, readersOfSelectedSeries);
  }

}
