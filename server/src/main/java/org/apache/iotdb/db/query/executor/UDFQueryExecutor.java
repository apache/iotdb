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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.UDFInputDataSet;
import org.apache.iotdb.db.query.dataset.UDTFAlignByTimeDataSet;
import org.apache.iotdb.db.query.dataset.UDTFNonAlignDataSet;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.series.ManagedSeriesReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.tsfile.read.query.executor.ExecutorWithTimeGenerator.markFilterdPaths;

public class UDFQueryExecutor extends RawDataQueryExecutor {

  protected final UDTFPlan udtfPlan;

  public UDFQueryExecutor(UDTFPlan udtfPlan) {
    super(udtfPlan);
    this.udtfPlan = udtfPlan;
  }

  public QueryDataSet executeWithoutValueFilterAlignByTime(QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException, InterruptedException {
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context);
    return new UDTFAlignByTimeDataSet(context, udtfPlan, readersOfSelectedSeries);
  }

  public QueryDataSet executeWithValueFilterAlignByTime(QueryContext context)
      throws StorageEngineException, QueryProcessException, IOException {
    // transfer to MeasurementPath to AlignedPath if it's under an aligned entity
    queryPlan.setDeduplicatedPaths(
        queryPlan.getDeduplicatedPaths().stream()
            .map(p -> ((MeasurementPath) p).transformToExactPath())
            .collect(Collectors.toList()));
    TimeGenerator timestampGenerator = getTimeGenerator(context, udtfPlan);
    List<Boolean> cached =
        markFilterdPaths(
            udtfPlan.getExpression(),
            new ArrayList<>(udtfPlan.getDeduplicatedPaths()),
            timestampGenerator.hasOrNode());
    Pair<List<IReaderByTimestamp>, List<List<Integer>>> pair =
        initSeriesReaderByTimestamp(context, udtfPlan, cached, timestampGenerator.getTimeFilter());
    return new UDTFAlignByTimeDataSet(
        context, udtfPlan, timestampGenerator, pair.left, pair.right, cached);
  }

  public QueryDataSet executeWithoutValueFilterNonAlign(QueryContext context)
      throws QueryProcessException, StorageEngineException, IOException, InterruptedException {
    List<ManagedSeriesReader> readersOfSelectedSeries = initManagedSeriesReader(context);
    return new UDTFNonAlignDataSet(context, udtfPlan, readersOfSelectedSeries);
  }

  public QueryDataSet executeWithValueFilterNonAlign(QueryContext context)
      throws QueryProcessException, StorageEngineException, IOException {
    // transfer to MeasurementPath to AlignedPath if it's under an aligned entity
    queryPlan.setDeduplicatedPaths(
        queryPlan.getDeduplicatedPaths().stream()
            .map(p -> ((MeasurementPath) p).transformToExactPath())
            .collect(Collectors.toList()));
    TimeGenerator timestampGenerator = getTimeGenerator(context, udtfPlan);
    List<Boolean> cached =
        markFilterdPaths(
            udtfPlan.getExpression(),
            new ArrayList<>(udtfPlan.getDeduplicatedPaths()),
            timestampGenerator.hasOrNode());
    Pair<List<IReaderByTimestamp>, List<List<Integer>>> pair =
        initSeriesReaderByTimestamp(context, udtfPlan, cached, timestampGenerator.getTimeFilter());
    return new UDTFNonAlignDataSet(
        context, udtfPlan, timestampGenerator, pair.left, pair.right, cached);
  }

  public final QueryDataSet executeFromAlignedDataSet(
      QueryContext context,
      QueryDataSet sourceDataSet,
      List<TSDataType> fieldTypes,
      boolean keepNull)
      throws QueryProcessException, IOException {
    return new UDTFAlignByTimeDataSet(
        context, udtfPlan, new UDFInputDataSet(sourceDataSet, fieldTypes), keepNull);
  }
}
