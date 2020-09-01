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


import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.FileLoaderUtils;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class LastQueryExecutor {
  private List<PartialPath> selectedSeries;
  private List<TSDataType> dataTypes;

  public LastQueryExecutor(LastQueryPlan lastQueryPlan) {
    this.selectedSeries = lastQueryPlan.getDeduplicatedPaths();
    this.dataTypes = lastQueryPlan.getDeduplicatedDataTypes();
  }

  public LastQueryExecutor(List<PartialPath> selectedSeries, List<TSDataType> dataTypes) {
    this.selectedSeries = selectedSeries;
    this.dataTypes = dataTypes;
  }

  /**
   * execute last function
   *
   * @param context query context
   */
  public QueryDataSet execute(QueryContext context, LastQueryPlan lastQueryPlan)
      throws StorageEngineException, IOException, QueryProcessException {

    ListDataSet dataSet = new ListDataSet(
        Arrays.asList(new PartialPath(COLUMN_TIMESERIES, false), new PartialPath(COLUMN_VALUE, false)),
            Arrays.asList(TSDataType.TEXT, TSDataType.TEXT));

    for (int i = 0; i < selectedSeries.size(); i++) {
      TimeValuePair lastTimeValuePair = null;
      try {
        lastTimeValuePair = calculateLastPairForOneSeries(
                new PartialPath(selectedSeries.get(i).getFullPath()), dataTypes.get(i), context,
                lastQueryPlan.getAllMeasurementsInDevice(selectedSeries.get(i).getDevice()));
      } catch (IllegalPathException e) {
        throw new QueryProcessException(e.getMessage());
      }
      if (lastTimeValuePair.getValue() != null) {
        RowRecord resultRecord = new RowRecord(lastTimeValuePair.getTimestamp());
        Field pathField = new Field(TSDataType.TEXT);
        if (selectedSeries.get(i).getTsAlias() != null) {
          pathField.setBinaryV(new Binary(selectedSeries.get(i).getTsAlias()));
        } else {
          if (selectedSeries.get(i).getMeasurementAlias() != null) {
            pathField.setBinaryV(new Binary(selectedSeries.get(i).getFullPathWithAlias()));
          } else {
            pathField.setBinaryV(new Binary(selectedSeries.get(i).getFullPath()));
          }
        }
        resultRecord.addField(pathField);

        Field valueField = new Field(TSDataType.TEXT);
        valueField.setBinaryV(new Binary(lastTimeValuePair.getValue().getStringValue()));
        resultRecord.addField(valueField);

        dataSet.putRecord(resultRecord);
      }
    }

    return dataSet;
  }

  protected TimeValuePair calculateLastPairForOneSeries(
      PartialPath seriesPath, TSDataType tsDataType, QueryContext context, Set<String> deviceMeasurements)
      throws IOException, QueryProcessException, StorageEngineException {
    return calculateLastPairForOneSeriesLocally(seriesPath, tsDataType, context,
        deviceMeasurements);
  }

  /**
   * get last result for one series
   *
   * @param context query context
   * @return TimeValuePair
   */
  public static TimeValuePair calculateLastPairForOneSeriesLocally(
      PartialPath seriesPath, TSDataType tsDataType, QueryContext context, Set<String> deviceMeasurements)
      throws IOException, QueryProcessException, StorageEngineException {

    // Retrieve last value from MNode
    MeasurementMNode node = null;
    try {
      node = (MeasurementMNode) IoTDB.metaManager.getNodeByPath(seriesPath);
    } catch (MetadataException e) {
      TimeValuePair timeValuePair = IoTDB.metaManager.getLastCache(seriesPath);
      if (timeValuePair != null) {
        return timeValuePair;
      }
    }

    if (node != null && node.getCachedLast() != null) {
      return node.getCachedLast();
    }

    QueryDataSource dataSource =
        QueryResourceManager.getInstance().getQueryDataSource(seriesPath, context, null);

    List<TsFileResource> seqFileResources = dataSource.getSeqResources();
    List<TsFileResource> unseqFileResources = dataSource.getUnseqResources();

    TimeValuePair resultPair = new TimeValuePair(Long.MIN_VALUE, null);

    if (!seqFileResources.isEmpty()) {
      for (int i = seqFileResources.size() - 1; i >= 0; i--) {
        TimeseriesMetadata timeseriesMetadata = FileLoaderUtils.loadTimeSeriesMetadata(
                seqFileResources.get(i), seriesPath, context, null, deviceMeasurements);
        if (timeseriesMetadata != null) {
          if (!timeseriesMetadata.isModified()) {
            Statistics timeseriesMetadataStats = timeseriesMetadata.getStatistics();
            resultPair = constructLastPair(
                    timeseriesMetadataStats.getEndTime(),
                    timeseriesMetadataStats.getLastValue(),
                    tsDataType);
            break;
          } else {
            List<ChunkMetadata> chunkMetadataList = timeseriesMetadata.loadChunkMetadataList();
            if (!chunkMetadataList.isEmpty()) {
              ChunkMetadata lastChunkMetaData = chunkMetadataList.get(chunkMetadataList.size() - 1);
              Statistics chunkStatistics = lastChunkMetaData.getStatistics();
              resultPair =
                  constructLastPair(
                      chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), tsDataType);
              break;
            }
          }
        }
      }
    }

    long version = 0;
    for (TsFileResource resource : unseqFileResources) {
      if (resource.getEndTime(seriesPath.getDevice()) < resultPair.getTimestamp()) {
        continue;
      }
      TimeseriesMetadata timeseriesMetadata =
          FileLoaderUtils.loadTimeSeriesMetadata(resource, seriesPath, context, null, deviceMeasurements);
      if (timeseriesMetadata != null) {
        for (ChunkMetadata chunkMetaData : timeseriesMetadata.loadChunkMetadataList()) {
          if (chunkMetaData.getEndTime() > resultPair.getTimestamp()
              || (chunkMetaData.getEndTime() == resultPair.getTimestamp()
              && chunkMetaData.getVersion() > version)) {
            Statistics chunkStatistics = chunkMetaData.getStatistics();
            resultPair =
                constructLastPair(
                    chunkStatistics.getEndTime(), chunkStatistics.getLastValue(), tsDataType);
            version = chunkMetaData.getVersion();
          }
        }
      }
    }

    // Update cached last value with low priority
    IoTDB.metaManager.updateLastCache(seriesPath,
      resultPair, false, Long.MIN_VALUE, node);
    return resultPair;
  }

  private static TimeValuePair constructLastPair(long timestamp, Object value, TSDataType dataType) {
    return new TimeValuePair(timestamp, TsPrimitiveType.getByType(dataType, value));
  }
}
