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

import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.IChunkLoader;
import org.apache.iotdb.tsfile.read.controller.IMetadataQuerier;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.iterator.EqualJoin;
import org.apache.iotdb.tsfile.read.query.iterator.LeftJoin;
import org.apache.iotdb.tsfile.read.query.iterator.MergeJoin;
import org.apache.iotdb.tsfile.read.query.iterator.SeriesIterator;
import org.apache.iotdb.tsfile.read.query.iterator.TimeSeries;
import org.apache.iotdb.tsfile.read.query.iterator.TimeSeriesQueryDataSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class ExecutorWithTimeGenerator2 implements QueryExecutor {

  private final IMetadataQuerier metadataQuerier;
  private final IChunkLoader chunkLoader;

  public ExecutorWithTimeGenerator2(IMetadataQuerier metadataQuerier, IChunkLoader chunkLoader) {
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
  public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
    IExpression expression = queryExpression.getExpression();
    List<Path> selectedPathList = queryExpression.getSelectedSeries();

    // TODO this should be dynamic, of course...

    Filter filter = ((SingleSeriesExpression) ((BinaryExpression.AndExpression) expression).left).getFilter();
    Filter filter2 = ((SingleSeriesExpression) ((BinaryExpression.AndExpression) expression).right).getFilter();

    TimeSeries s1 = new SeriesIterator(chunkLoader, metadataQuerier.getChunkMetaDataList(selectedPathList.get(0)), filter);
    TimeSeries s2 = new SeriesIterator(chunkLoader, metadataQuerier.getChunkMetaDataList(selectedPathList.get(1)), null);
    TimeSeries s4 = new SeriesIterator(chunkLoader, metadataQuerier.getChunkMetaDataList(selectedPathList.get(2)), filter2);
    TimeSeries s5 = new SeriesIterator(chunkLoader, metadataQuerier.getChunkMetaDataList(selectedPathList.get(3)), null);

    TimeSeries timeseries = new LeftJoin(new LeftJoin(new EqualJoin(s1, s4), s2), s5);

    return new TimeSeriesQueryDataSet(timeseries);
  }
}
