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

package org.apache.iotdb.db.query.dataset.groupby;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.impl.MinValueAggrResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.query.simpiece.TimeSeriesReader;
import org.apache.iotdb.db.query.simpiece.Visval;
import org.apache.iotdb.db.query.simpiece.VisvalPoint;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.MinMaxInfo;
import org.apache.iotdb.tsfile.read.common.ChunkSuit4Tri;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

public class LocalGroupByExecutorTri_Visval implements GroupByExecutor {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  // Aggregate result buffer of this path
  private final List<AggregateResult> results = new ArrayList<>();

  List<VisvalPoint> points;

  private final int m;

  public LocalGroupByExecutorTri_Visval(
      PartialPath path,
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      Filter timeFilter,
      TsFileFilter fileFilter,
      boolean ascending)
      throws StorageEngineException, QueryProcessException {
    //    long start = System.nanoTime();

    // get all data sources
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter);

    // update filter by TTL
    //    this.timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    SeriesReader seriesReader =
        new SeriesReader(
            path,
            allSensors,
            // fix bug: here use the aggregation type as the series data type,
            // not using pageReader.getAllSatisfiedPageData is ok
            dataType,
            context,
            queryDataSource,
            timeFilter,
            null,
            fileFilter,
            ascending);

    try {
      // : this might be bad to load all chunk metadata at first
      List<ChunkSuit4Tri> futureChunkList = new ArrayList<>();
      futureChunkList.addAll(seriesReader.getAllChunkMetadatas4Tri());
      // order futureChunkList by chunk startTime
      futureChunkList.sort(
          new Comparator<ChunkSuit4Tri>() {
            public int compare(ChunkSuit4Tri o1, ChunkSuit4Tri o2) {
              return ((Comparable) (o1.chunkMetadata.getStartTime()))
                  .compareTo(o2.chunkMetadata.getStartTime());
            }
          });

      GroupByFilter groupByFilter = (GroupByFilter) timeFilter;
      long startTime = groupByFilter.getStartTime();
      long endTime = groupByFilter.getEndTime();
      long interval = groupByFilter.getInterval();
      m = (int) Math.floor((endTime * 1.0 - startTime) / interval);

      points = TimeSeriesReader.getTimeSeriesFromTsFilesVisval(futureChunkList, startTime, endTime);

    } catch (IOException e) {
      throw new QueryProcessException(e.getMessage());
    }
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult) {
    results.add(aggrResult);
  }

  @Override
  public List<AggregateResult> calcResult(
      long curStartTime, long curEndTime, long startTime, long endTime, long interval)
      throws IOException {
    // group by curStartTime and curEndTime are not used in Sim-Piece segmentation

    StringBuilder series = new StringBuilder();

    // clear result cache
    for (AggregateResult result : results) {
      result.reset();
    }

    // Visval
    List<VisvalPoint> reducedPoints = Visval.reducePoints(points, m);

    for (VisvalPoint p : reducedPoints) {
      series.append(p.y).append("[").append(p.x).append("]").append(",");
    }

    MinValueAggrResult minValueAggrResult = (MinValueAggrResult) results.get(0);
    minValueAggrResult.updateResult(new MinMaxInfo<>(series.toString(), 0));

    points = null;
    return results;
  }

  @Override
  public Pair<Long, Object> peekNextNotNullValue(long nextStartTime, long nextEndTime)
      throws IOException {
    throw new IOException("no implemented");
  }

  @Override
  public List<AggregateResult> calcResult(long curStartTime, long curEndTime)
      throws IOException, QueryProcessException {
    throw new IOException("no implemented");
  }
}
