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

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;

import org.apache.iotdb.db.engine.cache.DeviceMetaDataCache;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.LeafMNode;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.query.reader.chunk.DiskChunkLoader;
import org.apache.iotdb.db.query.reader.series.SeriesLastReader;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.db.utils.QueryUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.io.IOException;
import java.util.*;

public class LastQueryExecutor {
  private List<Path> selectedSeries;
  private List<TSDataType> dataTypes;
  private IExpression optimizedExpression;

  public LastQueryExecutor(LastQueryPlan lastQueryPlan) {
    this.selectedSeries = lastQueryPlan.getPaths();
    this.dataTypes = lastQueryPlan.getDataTypes();
    this.optimizedExpression = lastQueryPlan.getExpression();
  }

  /**
   * execute last function
   *
   * @param context query context
   */
  public QueryDataSet execute(QueryContext context)
      throws StorageEngineException, IOException, QueryProcessException {
    Filter timeFilter = null;
    if (optimizedExpression != null) {
      timeFilter = ((GlobalTimeExpression) optimizedExpression).getFilter();
    }

    ListDataSet dataSet =
        new ListDataSet(
            Arrays.asList(new Path(COLUMN_TIMESERIES), new Path(COLUMN_VALUE)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));

    for (int i = 0; i < selectedSeries.size(); i++) {
      TimeValuePair lastTimeValuePair =
          calculateLastPairForOneSeries(selectedSeries.get(i), dataTypes.get(i), context, timeFilter);
      if (lastTimeValuePair.getValue() != null) {
        RowRecord resultRecord = new RowRecord(lastTimeValuePair.getTimestamp());
        Field pathField = new Field(TSDataType.TEXT);
        pathField.setBinaryV(new Binary(selectedSeries.get(i).getFullPath()));
        resultRecord.addField(pathField);

        Field valueField = new Field(TSDataType.TEXT);
        valueField.setBinaryV(new Binary(lastTimeValuePair.getValue().getStringValue()));
        resultRecord.addField(valueField);

        dataSet.putRecord(resultRecord);
      }
    }

    return dataSet;
  }

  /**
   * get last result for one series
   *
   * @param context query context
   * @return TimeValuePair
   */
  private TimeValuePair calculateLastPairForOneSeries(
      Path seriesPath, TSDataType tsDataType, QueryContext context, Filter timeFilter)
      throws IOException, QueryProcessException, StorageEngineException {

    // Retrieve last value from MNode
    MNode node = null;
    try {
      node = MManager.getInstance().getNodeByPath(seriesPath.toString());
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    if (((LeafMNode) node).getCachedLast() != null && timeFilter == null) {
      return ((LeafMNode) node).getCachedLast();
    }

    TimeValuePair resultPair = new TimeValuePair(Long.MIN_VALUE, null);
    QueryDataSource queryDataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, timeFilter);
    timeFilter = queryDataSource.updateFilterUsingTTL(timeFilter);

    SeriesLastReader seriesReader =
        new SeriesLastReader(
            seriesPath, tsDataType, context, queryDataSource, timeFilter, null, null);

    while (seriesReader.hasNextChunk()) {
      // cal by chunk statistics
      if (seriesReader.canUseCurrentChunkStatistics()) {
        Statistics chunkStatistics = seriesReader.currentChunkStatistics();
        resultPair =
            constructLastPair(
                chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), tsDataType);
        seriesReader.skipCurrentChunk();
      } else {
        BatchData lastBatchData = seriesReader.lastBatch();

        resultPair =
            new TimeValuePair(
                lastBatchData.getMaxTimestamp(),
                lastBatchData.getTsPrimitiveTypeByIndex(lastBatchData.length() - 1));
      }
    }

    // Update cached last value with low priority
    if (timeFilter == null)
      ((LeafMNode) node).updateCachedLast(resultPair, false, Long.MIN_VALUE);
    return resultPair;
  }

  private TimeValuePair constructLastPair(long timestamp, Object value, TSDataType dataType) {
    return new TimeValuePair(timestamp, TsPrimitiveType.getByType(dataType, value));
  }
}
