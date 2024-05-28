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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.db.queryengine.common.TimeseriesSchemaInfo;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ActiveTimeSeriesRegionScanOperator extends AbstractRegionScanDataSourceOperator {
  // Timeseries which need to be checked.
  private final Map<IDeviceID, Map<String, TimeseriesSchemaInfo>> timeSeriesToSchemasInfo;
  private static final Binary VIEW_TYPE = new Binary("BASE".getBytes());

  public ActiveTimeSeriesRegionScanOperator(
      OperatorContext operatorContext,
      PlanNodeId sourceId,
      Map<IDeviceID, Map<String, TimeseriesSchemaInfo>> timeSeriesToSchemasInfo,
      Filter timeFilter) {
    this.operatorContext = operatorContext;
    this.sourceId = sourceId;
    this.timeSeriesToSchemasInfo = timeSeriesToSchemasInfo;
    this.regionScanUtil = new RegionScanForActiveTimeSeriesUtil(timeFilter);
  }

  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }
    try {
      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      do {
        if (regionScanUtil.isCurrentTsFileFinished()
            && !((RegionScanForActiveTimeSeriesUtil) regionScanUtil)
                .nextTsFileHandle(timeSeriesToSchemasInfo)) {
          break;
        }

        if (regionScanUtil.filterChunkMetaData()) {
          continue;
        }

        if (regionScanUtil.filterChunkData()) {
          continue;
        }

        updateActiveTimeSeries();
        regionScanUtil.finishCurrentFile();

      } while (System.nanoTime() - start < maxRuntime && !resultTsBlockBuilder.isFull());

      finished =
          resultTsBlockBuilder.isEmpty()
              && (!regionScanUtil.hasMoreData() || timeSeriesToSchemasInfo.isEmpty());

      return !finished;
    } catch (IOException e) {
      throw new IOException("Error occurs when scanning active time series.", e);
    }
  }

  private void updateActiveTimeSeries() {
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();

    Map<IDeviceID, List<String>> activeTimeSeries =
        ((RegionScanForActiveTimeSeriesUtil) regionScanUtil).getActiveTimeSeries();
    for (Map.Entry<IDeviceID, List<String>> entry : activeTimeSeries.entrySet()) {
      IDeviceID deviceID = entry.getKey();
      String deviceStr = ((PlainDeviceID) deviceID).toStringID();
      List<String> timeSeriesList = entry.getValue();
      for (String timeSeries : timeSeriesList) {
        TimeseriesSchemaInfo schemaInfo = timeSeriesToSchemasInfo.get(deviceID).get(timeSeries);
        timeColumnBuilder.writeLong(-1);
        columnBuilders[0].writeBinary(
            new Binary(contactDeviceAndMeasurement(deviceStr, timeSeries)));
        columnBuilders[1].appendNull(); // alias
        columnBuilders[2].writeBinary(new Binary(schemaInfo.getDataBase().getBytes())); // Database
        columnBuilders[3].writeBinary(new Binary(schemaInfo.getDataType().getBytes())); // DataType
        columnBuilders[4].writeBinary(new Binary(schemaInfo.getEncoding().getBytes())); // Encoding
        columnBuilders[5].writeBinary(
            new Binary(schemaInfo.getCompression().getBytes())); // Compression

        if (schemaInfo.getTags() == null) {
          columnBuilders[6].appendNull(); // Tags
        } else {
          columnBuilders[6].writeBinary(new Binary(schemaInfo.getTags().getBytes())); // Tags
        }
        columnBuilders[7].appendNull(); // Attributes
        columnBuilders[8].writeBinary(new Binary(schemaInfo.getDeadband().getBytes())); // Deadband
        columnBuilders[9].writeBinary(
            new Binary(schemaInfo.getDeadbandParameters().getBytes())); // DeadbandParameters
        columnBuilders[10].writeBinary(VIEW_TYPE); // ViewType
        resultTsBlockBuilder.declarePosition();
        timeSeriesToSchemasInfo.get(deviceID).remove(timeSeries);
      }
      if (timeSeriesToSchemasInfo.get(deviceID).isEmpty()) {
        timeSeriesToSchemasInfo.remove(deviceID);
      }
    }
  }

  private byte[] contactDeviceAndMeasurement(String deviceStr, String measurementId) {
    return (deviceStr + "." + measurementId).getBytes();
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    return ColumnHeaderConstant.showTimeSeriesColumnHeaders.stream()
        .map(ColumnHeader::getColumnType)
        .collect(Collectors.toList());
  }
}
