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

import org.apache.iotdb.db.queryengine.common.TimeseriesContext;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeader;
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ActiveTimeSeriesRegionScanOperator extends AbstractRegionScanDataSourceOperator {
  // Timeseries which need to be checked.
  private final Map<IDeviceID, Map<String, TimeseriesContext>> timeSeriesToSchemasInfo;
  private static final Binary VIEW_TYPE = new Binary("BASE".getBytes());
  private final Binary dataBaseName;
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ActiveTimeSeriesRegionScanOperator.class)
          + RamUsageEstimator.shallowSizeOfInstance(Map.class)
          + RamUsageEstimator.shallowSizeOfInstance(Binary.class);

  public ActiveTimeSeriesRegionScanOperator(
      OperatorContext operatorContext,
      PlanNodeId sourceId,
      Map<IDeviceID, Map<String, TimeseriesContext>> timeSeriesToSchemasInfo,
      Filter timeFilter,
      Map<IDeviceID, Long> ttlCache,
      boolean isOutputCount) {
    this.outputCount = isOutputCount;
    this.operatorContext = operatorContext;
    this.sourceId = sourceId;
    this.timeSeriesToSchemasInfo = timeSeriesToSchemasInfo;
    this.regionScanUtil = new RegionScanForActiveTimeSeriesUtil(timeFilter, ttlCache);
    this.dataBaseName =
        new Binary(
            operatorContext
                .getDriverContext()
                .getFragmentInstanceContext()
                .getDataRegion()
                .getDatabaseName()
                .getBytes(TSFileConfig.STRING_CHARSET));
  }

  @Override
  protected boolean getNextTsFileHandle() throws IOException {
    return ((RegionScanForActiveTimeSeriesUtil) regionScanUtil)
        .nextTsFileHandle(timeSeriesToSchemasInfo);
  }

  @Override
  protected boolean isAllDataChecked() {
    return timeSeriesToSchemasInfo.isEmpty();
  }

  private void checkAndAppend(String info, ColumnBuilder columnBuilders) {
    if (info == null) {
      columnBuilders.appendNull();
    } else {
      columnBuilders.writeBinary(new Binary(info.getBytes(TSFileConfig.STRING_CHARSET)));
    }
  }

  @Override
  protected void updateActiveData() {
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();

    Map<IDeviceID, List<String>> activeTimeSeries =
        ((RegionScanForActiveTimeSeriesUtil) regionScanUtil).getActiveTimeSeries();

    if (outputCount) {
      for (Map.Entry<IDeviceID, List<String>> entry : activeTimeSeries.entrySet()) {
        List<String> timeSeriesList = entry.getValue();
        count += timeSeriesList.size();
        removeTimeseriesListFromDevice(entry.getKey(), timeSeriesList);
      }
      return;
    }

    for (Map.Entry<IDeviceID, List<String>> entry : activeTimeSeries.entrySet()) {
      IDeviceID deviceID = entry.getKey();
      String deviceStr = deviceID.toString();
      List<String> timeSeriesList = entry.getValue();
      Map<String, TimeseriesContext> timeSeriesInfo = timeSeriesToSchemasInfo.get(deviceID);
      for (String timeSeries : timeSeriesList) {
        TimeseriesContext schemaInfo = timeSeriesInfo.get(timeSeries);
        timeColumnBuilder.writeLong(-1);
        columnBuilders[0].writeBinary(
            new Binary(contactDeviceAndMeasurement(deviceStr, timeSeries)));

        checkAndAppend(schemaInfo.getAlias(), columnBuilders[1]); // Measurement
        columnBuilders[2].writeBinary(dataBaseName); // Database
        checkAndAppend(schemaInfo.getDataType(), columnBuilders[3]); // DataType
        checkAndAppend(schemaInfo.getEncoding(), columnBuilders[4]); // Encoding
        checkAndAppend(schemaInfo.getCompression(), columnBuilders[5]); // Compression
        checkAndAppend(schemaInfo.getTags(), columnBuilders[6]); // Tags
        checkAndAppend(schemaInfo.getAttributes(), columnBuilders[7]); // Attributes
        checkAndAppend(schemaInfo.getDeadband(), columnBuilders[8]); // Description
        checkAndAppend(schemaInfo.getDeadbandParameters(), columnBuilders[9]); // DeadbandParameters
        columnBuilders[10].writeBinary(VIEW_TYPE); // ViewType
        resultTsBlockBuilder.declarePosition();
      }
      removeTimeseriesListFromDevice(deviceID, timeSeriesList);
    }
  }

  private void removeTimeseriesListFromDevice(IDeviceID deviceID, List<String> timeSeriesList) {
    Map<String, TimeseriesContext> timeSeriesInfo = timeSeriesToSchemasInfo.get(deviceID);
    for (String timeSeries : timeSeriesList) {
      timeSeriesInfo.remove(timeSeries);
    }
    if (timeSeriesInfo.isEmpty()) {
      timeSeriesToSchemasInfo.remove(deviceID);
    }
  }

  private byte[] contactDeviceAndMeasurement(String deviceStr, String measurementId) {
    return (deviceStr + "." + measurementId).getBytes(TSFileConfig.STRING_CHARSET);
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    if (outputCount) {
      return ColumnHeaderConstant.countTimeSeriesColumnHeaders.stream()
          .map(ColumnHeader::getColumnType)
          .collect(Collectors.toList());
    }
    return ColumnHeaderConstant.showTimeSeriesColumnHeaders.stream()
        .map(ColumnHeader::getColumnType)
        .collect(Collectors.toList());
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed() + INSTANCE_SIZE;
  }
}
