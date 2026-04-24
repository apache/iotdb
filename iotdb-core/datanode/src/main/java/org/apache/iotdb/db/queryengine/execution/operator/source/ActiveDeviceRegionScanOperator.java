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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.schema.utils.MeasurementPropsUtils;
import org.apache.iotdb.db.queryengine.common.DeviceContext;
import org.apache.iotdb.db.queryengine.common.TimeseriesContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.analyze.cache.schema.DataNodeTTLCache;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ActiveDeviceRegionScanOperator extends AbstractRegionScanDataSourceOperator {

  private final Map<IDeviceID, DeviceContext> deviceContextMap;

  private final Map<IDeviceID, Map<String, TimeseriesContext>> timeSeriesToSchemasInfo;

  private final Set<IDeviceID> processedDevices = new HashSet<>();

  private final boolean useTimeSeriesLevelScan;

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ActiveDeviceRegionScanOperator.class)
          + RamUsageEstimator.shallowSizeOfInstance(Map.class);

  public ActiveDeviceRegionScanOperator(
      OperatorContext operatorContext,
      PlanNodeId sourceId,
      Map<IDeviceID, DeviceContext> deviceContextMap,
      Filter timeFilter,
      Map<IDeviceID, Long> ttlCache,
      boolean outputCount,
      boolean useTimeSeriesLevelScan,
      Map<IDeviceID, Map<String, TimeseriesContext>> timeSeriesToSchemasInfo) {
    this.outputCount = outputCount;
    this.sourceId = sourceId;
    this.operatorContext = operatorContext;
    this.useTimeSeriesLevelScan = useTimeSeriesLevelScan;
    this.deviceContextMap = deviceContextMap;
    this.timeSeriesToSchemasInfo = timeSeriesToSchemasInfo;
    if (useTimeSeriesLevelScan) {
      this.regionScanUtil = new RegionScanForActiveTimeSeriesUtil(timeFilter, ttlCache);
    } else {
      this.regionScanUtil = new RegionScanForActiveDeviceUtil(timeFilter, ttlCache);
    }
  }

  @Override
  protected boolean getNextTsFileHandle() throws IOException, IllegalPathException {
    if (useTimeSeriesLevelScan) {
      return ((RegionScanForActiveTimeSeriesUtil) regionScanUtil)
          .nextTsFileHandle(timeSeriesToSchemasInfo);
    } else {
      return ((RegionScanForActiveDeviceUtil) regionScanUtil).nextTsFileHandle(deviceContextMap);
    }
  }

  @Override
  protected boolean isAllDataChecked() {
    if (useTimeSeriesLevelScan) {
      return timeSeriesToSchemasInfo.isEmpty();
    } else {
      return deviceContextMap.isEmpty();
    }
  }

  @Override
  protected void updateActiveData() {
    if (useTimeSeriesLevelScan) {
      updateActiveDataFromTimeSeries();
    } else {
      updateActiveDataFromDevice();
    }
  }

  private void updateActiveDataFromDevice() {
    List<IDeviceID> activeDevices =
        ((RegionScanForActiveDeviceUtil) regionScanUtil).getActiveDevices();

    if (this.outputCount) {
      count += activeDevices.size();
      activeDevices.forEach(deviceContextMap.keySet()::remove);
    } else {
      TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
      ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
      for (IDeviceID deviceID : activeDevices) {
        DeviceContext deviceContext = deviceContextMap.get(deviceID);
        int templateId = deviceContext.getTemplateId();
        // TODO: use IDeviceID interface to get ttl
        long ttl = DataNodeTTLCache.getInstance().getTTLForTree(deviceID);
        // TODO: make it more readable, like "30 days" or "10 hours"
        String ttlStr = ttl == Long.MAX_VALUE ? IoTDBConstant.TTL_INFINITE : String.valueOf(ttl);

        timeColumnBuilder.writeLong(-1);
        columnBuilders[0].writeBinary(new Binary(deviceID.toString(), TSFileConfig.STRING_CHARSET));
        columnBuilders[1].writeBinary(
            new Binary(
                String.valueOf(deviceContextMap.get(deviceID).isAligned()),
                TSFileConfig.STRING_CHARSET));

        if (templateId != SchemaConstant.NON_TEMPLATE) {
          columnBuilders[2].writeBinary(
              new Binary(
                  String.valueOf(
                      ClusterTemplateManager.getInstance().getTemplate(templateId).getName()),
                  TSFileConfig.STRING_CHARSET));
        } else {
          columnBuilders[2].appendNull();
        }
        columnBuilders[3].writeBinary(new Binary(ttlStr, TSFileConfig.STRING_CHARSET));
        resultTsBlockBuilder.declarePosition();
        deviceContextMap.remove(deviceID);
      }
    }
  }

  private void updateActiveDataFromTimeSeries() {
    final Map<IDeviceID, List<String>> activeTimeSeries =
        ((RegionScanForActiveTimeSeriesUtil) regionScanUtil).getActiveTimeSeries();

    if (this.outputCount) {
      processActiveTimeSeriesForCount(activeTimeSeries);
    } else {
      processActiveTimeSeriesForOutput(activeTimeSeries);
    }
  }

  private void processActiveTimeSeriesForCount(Map<IDeviceID, List<String>> activeTimeSeries) {
    for (Map.Entry<IDeviceID, List<String>> entry : activeTimeSeries.entrySet()) {
      final IDeviceID deviceID = entry.getKey();
      final Map<String, TimeseriesContext> schemas = timeSeriesToSchemasInfo.get(deviceID);
      final Set<IDeviceID> uniqueDevices = new HashSet<>();
      for (String value : entry.getValue()) {
        IDeviceID aliasDeviceID = processTimeSeriesValue(deviceID, value, schemas);
        if (aliasDeviceID != null && !processedDevices.contains(aliasDeviceID)) {
          uniqueDevices.add(aliasDeviceID);
          processedDevices.add(deviceID);
        }
      }
      cleanupEmptySchemas(deviceID);
      count += uniqueDevices.size();
    }
  }

  private void processActiveTimeSeriesForOutput(Map<IDeviceID, List<String>> activeTimeSeries) {
    TimeColumnBuilder timeColumnBuilder = resultTsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] columnBuilders = resultTsBlockBuilder.getValueColumnBuilders();
    Map<IDeviceID, DeviceContext> uniqueDevices = new HashMap<>();

    for (Map.Entry<IDeviceID, List<String>> entry : activeTimeSeries.entrySet()) {
      IDeviceID deviceID = entry.getKey();
      Map<String, TimeseriesContext> schemas = timeSeriesToSchemasInfo.get(deviceID);
      for (String value : entry.getValue()) {
        IDeviceID aliasDeviceID = processTimeSeriesValue(deviceID, value, schemas);
        if (aliasDeviceID != null
            && !processedDevices.contains(aliasDeviceID)
            && !uniqueDevices.containsKey(aliasDeviceID)) {
          DeviceContext deviceContext = deviceContextMap.get(aliasDeviceID);
          processedDevices.add(deviceID);
          uniqueDevices.put(
              aliasDeviceID,
              deviceContext != null
                  ? deviceContext
                  : new DeviceContext(false, SchemaConstant.NON_TEMPLATE));
        }
      }
      cleanupEmptySchemas(deviceID);
    }

    for (Map.Entry<IDeviceID, DeviceContext> deviceEntry : uniqueDevices.entrySet()) {
      writeDeviceToResult(
          deviceEntry.getKey(), deviceEntry.getValue(), timeColumnBuilder, columnBuilders);
    }
  }

  private IDeviceID processTimeSeriesValue(
      IDeviceID deviceID, String value, Map<String, TimeseriesContext> schemas) {
    TimeseriesContext timeseriesContext = schemas.remove(value);
    if (timeseriesContext == null) {
      return null;
    }
    if (MeasurementPropsUtils.isInvalid(timeseriesContext.getProps())) {
      PartialPath aliasPath = MeasurementPropsUtils.getAliasPath(timeseriesContext.getProps());
      if (aliasPath == null) {
        return null;
      }
      return aliasPath.getIDeviceID();
    }
    return deviceID;
  }

  private void cleanupEmptySchemas(IDeviceID deviceID) {
    if (timeSeriesToSchemasInfo.get(deviceID).isEmpty()) {
      timeSeriesToSchemasInfo.remove(deviceID);
    }
  }

  private void writeDeviceToResult(
      IDeviceID deviceID,
      DeviceContext deviceContext,
      TimeColumnBuilder timeColumnBuilder,
      ColumnBuilder[] columnBuilders) {
    timeColumnBuilder.writeLong(-1);
    columnBuilders[0].writeBinary(new Binary(deviceID.toString(), TSFileConfig.STRING_CHARSET));
    columnBuilders[1].writeBinary(
        new Binary(String.valueOf(deviceContext.isAligned()), TSFileConfig.STRING_CHARSET));

    int templateId = deviceContext.getTemplateId();
    if (templateId != SchemaConstant.NON_TEMPLATE) {
      columnBuilders[2].writeBinary(
          new Binary(
              String.valueOf(
                  ClusterTemplateManager.getInstance().getTemplate(templateId).getName()),
              TSFileConfig.STRING_CHARSET));
    } else {
      columnBuilders[2].appendNull();
    }
    long ttl = DataNodeTTLCache.getInstance().getTTLForTree(deviceID);
    String ttlStr = ttl == Long.MAX_VALUE ? IoTDBConstant.TTL_INFINITE : String.valueOf(ttl);
    columnBuilders[3].writeBinary(new Binary(ttlStr, TSFileConfig.STRING_CHARSET));
    resultTsBlockBuilder.declarePosition();
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    if (outputCount) {
      return ColumnHeaderConstant.countDevicesColumnHeaders.stream()
          .map(ColumnHeader::getColumnType)
          .collect(Collectors.toList());
    }
    return ColumnHeaderConstant.showDevicesColumnHeaders.stream()
        .map(ColumnHeader::getColumnType)
        .collect(Collectors.toList());
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed() + INSTANCE_SIZE;
  }
}
