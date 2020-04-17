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
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.mnode.LeafMNode;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;

public class LastQueryExecutor {
  private List<Path> selectedSeries;
  private List<TSDataType> dataTypes;

  public LastQueryExecutor(LastQueryPlan lastQueryPlan) {
    this.selectedSeries = lastQueryPlan.getDeduplicatedPaths();
    this.dataTypes = lastQueryPlan.getDeduplicatedDataTypes();
  }

  public LastQueryExecutor(List<Path> selectedSeries, List<TSDataType> dataTypes) {
    this.selectedSeries = selectedSeries;
    this.dataTypes = dataTypes;
  }

  /**
   * execute last function
   *
   * @param context query context
   */
  public QueryDataSet execute(QueryContext context)
      throws StorageEngineException, IOException, QueryProcessException {

    ListDataSet dataSet = new ListDataSet(
        Arrays.asList(new Path(COLUMN_TIMESERIES), new Path(COLUMN_VALUE)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));

    for (int i = 0; i < selectedSeries.size(); i++) {
      TimeValuePair lastTimeValuePair =
          calculateLastPairForOneSeries(selectedSeries.get(i), dataTypes.get(i), context);
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
  public static TimeValuePair calculateLastPairForOneSeries(
      Path seriesPath, TSDataType tsDataType, QueryContext context)
      throws IOException, QueryProcessException, StorageEngineException {

    // Retrieve last value from MNode
    LeafMNode node;
    try {
      node = (LeafMNode) MManager.getInstance().getNodeByPath(seriesPath.toString());
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    if (node.getCachedLast() != null) {
      return node.getCachedLast();
    }

    QueryDataSource dataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, null);

    List<TsFileResource> seqFileResources = dataSource.getSeqResources();
    List<TsFileResource> unseqFileResources = dataSource.getUnseqResources();

    TimeValuePair resultPair = new TimeValuePair(Long.MIN_VALUE, null);

    if (!seqFileResources.isEmpty()) {
      for (int i = seqFileResources.size() - 1; i >= 0; i--) {
        List<ChunkMetadata> chunkMetadata = FileLoaderUtils.loadChunkMetadataFromTsFileResource(
                seqFileResources.get(i), seriesPath, context);
        if (!chunkMetadata.isEmpty()) {
          ChunkMetadata lastChunkMetaData = chunkMetadata.get(chunkMetadata.size() - 1);
          Statistics chunkStatistics = lastChunkMetaData.getStatistics();
          resultPair = constructLastPair(
                  chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), tsDataType);
          break;
        }
      }
    }

    long version = 0;
    for (TsFileResource resource : unseqFileResources) {
      if (resource.getEndTimeMap().get(seriesPath.getDevice()) < resultPair.getTimestamp()) {
        continue;
      }
      List<ChunkMetadata> chunkMetadata =
          FileLoaderUtils.loadChunkMetadataFromTsFileResource(resource, seriesPath, context);
      for (ChunkMetadata chunkMetaData : chunkMetadata) {
        if (chunkMetaData.getEndTime() == resultPair.getTimestamp()
            && chunkMetaData.getVersion() > version) {
          Statistics chunkStatistics = chunkMetaData.getStatistics();
          resultPair = constructLastPair(
                  chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), tsDataType);
          version = chunkMetaData.getVersion();
        }
      }
    }

    // Update cached last value with low priority
    node.updateCachedLast(resultPair, false, Long.MIN_VALUE);
    return resultPair;
  }

  private static TimeValuePair constructLastPair(long timestamp, Object value, TSDataType dataType) {
    return new TimeValuePair(timestamp, TsPrimitiveType.getByType(dataType, value));
  }
}
