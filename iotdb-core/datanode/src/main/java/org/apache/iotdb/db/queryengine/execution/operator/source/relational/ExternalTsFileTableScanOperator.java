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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.ReadTsFileTableFunction.ExternalTsFileDeviceOffset;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.FileLoaderUtils;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExternalTsFileTableScanOperator extends TableScanOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ExternalTsFileTableScanOperator.class);
  private static final long ABSTRACT_DEVICE_TABLE_SCAN_OPERATOR_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(AbstractDeviceTableScanOperator.class);

  private final String tableName;
  private final List<List<ExternalTsFileDeviceOffset>> deviceOffsets;

  public ExternalTsFileTableScanOperator(
      AbstractTableScanOperatorParameter parameter,
      String tableName,
      List<List<ExternalTsFileDeviceOffset>> deviceOffsets) {
    super(parameter);
    this.tableName = tableName;
    this.deviceOffsets = new ArrayList<>(deviceOffsets);
    if (deviceCount != this.deviceOffsets.size()) {
      throw new IllegalArgumentException(
          "The size of external TsFile device offsets should be equal to device entries");
    }
  }

  @Override
  String getNthIdColumnValue(DeviceEntry deviceEntry, int idColumnIndex) {
    int segmentOffset =
        deviceEntry.getDeviceID().segmentNum() > 0
                && tableName.equalsIgnoreCase((String) deviceEntry.getNthSegment(0))
            ? 1
            : 0;
    Object segment = deviceEntry.getNthSegment(idColumnIndex + segmentOffset);
    return segment == null ? null : (String) segment;
  }

  @Override
  protected void constructAlignedSeriesScanUtil() {
    if (!hasCurrentDeviceEntry()) {
      return;
    }

    DeviceEntry deviceEntry = getCurrentDeviceEntry();
    if (deviceEntry == null) {
      throw new IllegalStateException("Current device entry in TableScanOperator is empty");
    }

    this.seriesScanUtil =
        new ExternalTsFileSeriesScanUtil(
            constructAlignedPath(
                deviceEntry, measurementColumnNames, measurementSchemas, allSensors),
            scanOrder,
            seriesScanOptions,
            ((OperatorContext) operatorContext).getInstanceContext(),
            true,
            measurementColumnTSDataTypes,
            this::loadTimeSeriesMetadata);
  }

  private AbstractAlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource, AlignedFullPath alignedPath) throws IOException {
    List<ExternalTsFileDeviceOffset> currentDeviceOffsets = deviceOffsets.get(currentDeviceIndex);
    if (currentDeviceOffsets == null
        || !getCurrentDeviceEntry().getDeviceID().equals(alignedPath.getDeviceId())) {
      return null;
    }

    long[] deviceMeasurementNodeOffset =
        getDeviceMeasurementNodeOffset(currentDeviceOffsets, resource.getTsFilePath());
    if (deviceMeasurementNodeOffset == null) {
      return null;
    }
    // TODO: Use deviceMeasurementNodeOffset after FileLoaderUtils supports offset-based metadata
    // loading in this branch.
    return FileLoaderUtils.loadAlignedTimeSeriesMetadata(
        resource,
        alignedPath,
        ((OperatorContext) operatorContext).getInstanceContext(),
        seriesScanOptions.getGlobalTimeFilter(),
        resource.isSeq(),
        ((OperatorContext) operatorContext).getInstanceContext().isIgnoreAllNullRows());
  }

  private long[] getDeviceMeasurementNodeOffset(
      List<ExternalTsFileDeviceOffset> currentDeviceOffsets, String tsFilePath) {
    for (ExternalTsFileDeviceOffset offset : currentDeviceOffsets) {
      if (tsFilePath.equals(offset.getTsFilePath())) {
        return offset.getDeviceMeasurementNodeOffset();
      }
    }
    return null;
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed()
        + INSTANCE_SIZE
        - ABSTRACT_DEVICE_TABLE_SCAN_OPERATOR_INSTANCE_SIZE
        + RamUsageEstimator.sizeOfCollection(deviceOffsets);
  }
}
