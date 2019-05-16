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
package org.apache.iotdb.cluster.query.executor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.cluster.query.manager.coordinatornode.ClusterRpcSingleQueryManager;
import org.apache.iotdb.cluster.query.reader.coordinatornode.ClusterSelectSeriesReader;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.query.aggregation.AggreResultData;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.aggregation.impl.LastAggrFunc;
import org.apache.iotdb.db.query.aggregation.impl.MaxTimeAggrFunc;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.AggreResultDataPointReader;
import org.apache.iotdb.db.query.dataset.EngineDataSetWithoutTimeGenerator;
import org.apache.iotdb.db.query.executor.AggregateEngineExecutor;
import org.apache.iotdb.db.query.factory.AggreFuncFactory;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class ClusterAggregateEngineExecutor extends AggregateEngineExecutor {

  private ClusterRpcSingleQueryManager queryManager;

  public ClusterAggregateEngineExecutor(List<Path> selectedSeries, List<String> aggres,
      IExpression expression, ClusterRpcSingleQueryManager queryManager) {
    super(selectedSeries, aggres, expression);
    this.queryManager = queryManager;
  }

  @Override
  public QueryDataSet executeWithoutTimeGenerator(QueryContext context)
      throws FileNodeManagerException, IOException, PathErrorException, ProcessorException {
    Filter timeFilter = expression != null ? ((GlobalTimeExpression) expression).getFilter() : null;
    Map<Path, ClusterSelectSeriesReader> selectPathReaders = queryManager.getSelectSeriesReaders();

    List<Path> paths = new ArrayList<>();
    List<IPointReader> readers = new ArrayList<>();
    List<TSDataType> dataTypes = new ArrayList<>();
    for (int i = 0; i < selectedSeries.size(); i++) {
      Path path = selectedSeries.get(i);

      if (selectPathReaders.containsKey(path)) {
        ClusterSelectSeriesReader reader = selectPathReaders.get(path);
        readers.add(reader);
        dataTypes.add(reader.getDataType());
      } else {
        paths.add(path);
        // construct AggregateFunction
        TSDataType tsDataType = MManager.getInstance()
            .getSeriesType(selectedSeries.get(i).getFullPath());
        AggregateFunction function = AggreFuncFactory.getAggrFuncByName(aggres.get(i), tsDataType);
        function.init();

        QueryDataSource queryDataSource = QueryResourceManager.getInstance()
            .getQueryDataSource(selectedSeries.get(i), context);

        // sequence reader for sealed tsfile, unsealed tsfile, memory
        SequenceDataReader sequenceReader;
        if (function instanceof MaxTimeAggrFunc || function instanceof LastAggrFunc) {
          sequenceReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), timeFilter,
              context, true);
        } else {
          sequenceReader = new SequenceDataReader(queryDataSource.getSeqDataSource(), timeFilter,
              context, false);
        }

        // unseq reader for all chunk groups in unSeqFile, memory
        PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance()
            .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), timeFilter);

        AggreResultData aggreResultData = aggregateWithoutTimeGenerator(function,
            sequenceReader, unSeqMergeReader, timeFilter);
        readers.add(new AggreResultDataPointReader(aggreResultData));
      }
    }
    QueryResourceManager.getInstance()
        .beginQueryOfGivenQueryPaths(context.getJobId(), paths);

    return new EngineDataSetWithoutTimeGenerator(selectedSeries, dataTypes, readers);
  }
}
