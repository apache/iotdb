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
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.readTsFile.ExternalTsFileQueryResource;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.readTsFile.ExternalTsFileQueryResource.DeviceOffset;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.readTsFile.ExternalTsFileQueryResource.MultiWayMergeReader;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;

public class ExternalTsFileAggTableScanOperator extends DefaultAggTableScanOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ExternalTsFileAggTableScanOperator.class);

  private final String tableName;
  private final ExternalTsFileQueryResource externalTsFileQueryResource;
  private final int deviceTaskPartitionIndex;
  private MultiWayMergeReader deviceTaskReader;
  private int loadedDeviceOffsetIndex = -1;
  private List<DeviceOffset> currentDeviceOffsets = Collections.emptyList();

  public ExternalTsFileAggTableScanOperator(
      AbstractAggTableScanOperatorParameter parameter,
      String tableName,
      ExternalTsFileQueryResource externalTsFileQueryResource,
      int deviceTaskPartitionIndex) {
    super(parameter);
    this.tableName = tableName;
    this.externalTsFileQueryResource = externalTsFileQueryResource;
    this.deviceTaskPartitionIndex = deviceTaskPartitionIndex;
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
    DeviceEntry deviceEntry =
        deviceEntries.isEmpty() || deviceEntries.get(currentDeviceIndex) == null
            ? new AlignedDeviceEntry(SeriesScanUtil.EMPTY_DEVICE_ID, new Binary[0])
            : deviceEntries.get(currentDeviceIndex);
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
    if (deviceEntries.isEmpty() || currentDeviceIndex >= deviceEntries.size()) {
      return null;
    }
    return ExternalTsFileSeriesScanUtil.loadTimeSeriesMetadata(
        resource,
        alignedPath,
        deviceEntries.get(currentDeviceIndex).getDeviceID(),
        getCurrentDeviceOffsets(),
        externalTsFileQueryResource.getTsFilePaths(),
        ((OperatorContext) operatorContext).getInstanceContext(),
        seriesScanOptions.getGlobalTimeFilter());
  }

  private List<DeviceOffset> getCurrentDeviceOffsets() throws IOException {
    if (loadedDeviceOffsetIndex == currentDeviceIndex) {
      return currentDeviceOffsets;
    }
    if (deviceTaskReader == null) {
      deviceTaskReader =
          externalTsFileQueryResource.getMultiWayMergeReader(deviceTaskPartitionIndex);
    }
    DeviceEntry currentDeviceEntry = deviceEntries.get(currentDeviceIndex);
    while (deviceTaskReader.hasNextDevice()) {
      DeviceEntry deviceEntry = deviceTaskReader.nextDevice();
      if (deviceEntry.getDeviceID().equals(currentDeviceEntry.getDeviceID())) {
        currentDeviceOffsets = deviceTaskReader.getCurrentDeviceOffsets();
        loadedDeviceOffsetIndex = currentDeviceIndex;
        return currentDeviceOffsets;
      }
    }
    currentDeviceOffsets = Collections.emptyList();
    loadedDeviceOffsetIndex = currentDeviceIndex;
    return currentDeviceOffsets;
  }

  @Override
  public void close() throws Exception {
    if (deviceTaskReader != null) {
      deviceTaskReader.close();
      deviceTaskReader = null;
    }
    super.close();
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed()
        + INSTANCE_SIZE
        - AbstractDefaultAggTableScanOperator.INSTANCE_SIZE
        + RamUsageEstimator.sizeOfCollection(currentDeviceOffsets);
  }
}
