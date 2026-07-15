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

import org.apache.iotdb.calc.plan.planner.CommonOperatorUtils;
import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryDataSource;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource.DeviceTaskRunReader;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;

public class ExternalTsFileTableScanOperator extends TableScanOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ExternalTsFileTableScanOperator.class);

  private final int deviceTaskPartitionIndex;
  private DeviceTaskRunReader deviceTaskReader;

  public ExternalTsFileTableScanOperator(
      AbstractTableScanOperatorParameter parameter, int deviceTaskPartitionIndex) {
    super(parameter);
    this.deviceTaskPartitionIndex = deviceTaskPartitionIndex;
  }

  @Override
  protected void constructAlignedSeriesScanUtil() {
    if (currentDeviceIndex >= deviceCount) {
      return;
    }

    DeviceEntry deviceEntry = deviceEntries.get(currentDeviceIndex);
    if (deviceEntry == null) {
      throw new IllegalStateException(
          DataNodeQueryMessages.CURRENT_DEVICE_ENTRY_IN_TABLESCANOPERATOR_IS_EMPTY);
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
    return ExternalTsFileSeriesScanUtil.loadTimeSeriesMetadata(
        resource,
        alignedPath,
        deviceEntries.get(currentDeviceIndex).getDeviceID(),
        deviceTaskReader.getCurrentDeviceOffsetMap().get(resource),
        ((OperatorContext) operatorContext).getInstanceContext(),
        seriesScanOptions.getGlobalTimeFilter());
  }

  @Override
  public void initQueryDataSource(IQueryDataSource dataSource) {
    ExternalTsFileQueryResource externalTsFileQueryResource =
        ((ExternalTsFileQueryDataSource) dataSource).getExternalTsFileQueryResource();
    if (deviceTaskReader == null) {
      deviceTaskReader =
          externalTsFileQueryResource.getDeviceTaskRunReader(deviceTaskPartitionIndex);
    }
    IQueryDataSource currentDataSource =
        currentDeviceIndex < deviceCount ? updateCurrentDeviceQueryDataSource() : dataSource;
    super.initQueryDataSource(currentDataSource);
  }

  @Override
  protected boolean shouldStopScanByRuntimeFilter() {
    // Each device uses its own QueryDataSource; exhausting files for a non-last device must not
    // stop scanning remaining devices in the same external TsFile task.
    if (currentDeviceIndex < deviceCount - 1) {
      return false;
    }
    return super.shouldStopScanByRuntimeFilter();
  }

  @Override
  protected void moveToNextDevice() {
    currentDeviceIndex++;
    if (currentDeviceIndex < deviceCount) {
      constructAlignedSeriesScanUtil();
      queryDataSource = updateCurrentDeviceQueryDataSource();
      seriesScanUtil.initQueryDataSource(queryDataSource);
      this.operatorContext.recordSpecifiedInfo(
          CommonOperatorUtils.CURRENT_DEVICE_INDEX_STRING, Integer.toString(currentDeviceIndex));
    }
  }

  private QueryDataSource updateCurrentDeviceQueryDataSource() {
    try {
      if (!deviceTaskReader.nextDevice()) {
        throw new IllegalStateException(
            DataNodeQueryMessages
                    .UNEXPECTED_END_OF_EXTERNAL_TSFILE_DEVICE_TASK_READER_AT_DEVICE_INDEX
                + currentDeviceIndex);
      }
      DeviceEntry expectedDeviceEntry = deviceEntries.get(currentDeviceIndex);
      DeviceEntry currentDeviceEntry = deviceTaskReader.getCurrentDevice();
      if (!expectedDeviceEntry.getDeviceID().equals(currentDeviceEntry.getDeviceID())) {
        throw new IllegalStateException(
            String.format(
                DataNodeQueryMessages
                    .EXTERNAL_TSFILE_DEVICE_TASK_READER_IS_NOT_ALIGNED_WITH_DEVICE_ENTRIES,
                currentDeviceIndex,
                expectedDeviceEntry.getDeviceID(),
                currentDeviceEntry.getDeviceID()));
      }
      return deviceTaskReader.getCurrentDeviceQueryDataSource();
    } catch (IOException e) {
      throw new RuntimeException(
          DataNodeQueryMessages.FAILED_TO_UPDATE_EXTERNAL_TSFILE_DEVICE_RESOURCES, e);
    }
  }

  @Override
  public void close() throws Exception {
    Exception exception = null;
    try {
      if (deviceTaskReader != null) {
        deviceTaskReader.close();
      }
    } catch (Exception e) {
      exception = e;
    } finally {
      deviceTaskReader = null;
      try {
        super.close();
      } catch (Exception e) {
        if (exception == null) {
          exception = e;
        } else {
          exception.addSuppressed(e);
        }
      }
    }

    if (exception != null) {
      throw exception;
    }
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed() + INSTANCE_SIZE - AbstractTableScanOperator.INSTANCE_SIZE;
  }
}
