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
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryDataSource;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource.DeviceTaskRunReader;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;

import static org.apache.iotdb.db.queryengine.execution.operator.source.relational.TableScanOperator.constructAlignedPath;

public class ExternalTsFileAggTableScanOperator extends DefaultAggTableScanOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(ExternalTsFileAggTableScanOperator.class);

  private final int deviceTaskPartitionIndex;
  private DeviceTaskRunReader deviceTaskReader;

  public ExternalTsFileAggTableScanOperator(
      AbstractAggTableScanOperatorParameter parameter, int deviceTaskPartitionIndex) {
    super(parameter);
    this.deviceTaskPartitionIndex = deviceTaskPartitionIndex;
  }

  @Override
  protected void constructAlignedSeriesScanUtil() {
    DeviceEntry deviceEntry =
        !hasCurrentRealDeviceEntry()
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
    if (!hasCurrentRealDeviceEntry()) {
      return null;
    }
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
    if (hasCurrentRealDeviceEntry() && deviceTaskReader == null) {
      deviceTaskReader =
          externalTsFileQueryResource.getDeviceTaskRunReader(deviceTaskPartitionIndex);
    }
    super.initQueryDataSource(
        hasCurrentRealDeviceEntry() ? updateCurrentDeviceQueryDataSource() : dataSource);
  }

  @Override
  protected void moveToNextDevice() throws Exception {
    nextDevice();
    inputTsBlock = null;

    if (currentDeviceIndex < deviceCount) {
      constructAlignedSeriesScanUtil();
      queryDataSource =
          hasCurrentRealDeviceEntry() ? updateCurrentDeviceQueryDataSource() : queryDataSource;
      seriesScanUtil.initQueryDataSource(queryDataSource);
    }

    if (currentDeviceIndex >= deviceCount) {
      timeIterator.setFinished();
    }
  }

  private boolean hasCurrentRealDeviceEntry() {
    return !deviceEntries.isEmpty()
        && currentDeviceIndex < deviceEntries.size()
        && deviceEntries.get(currentDeviceIndex) != null;
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
    return super.ramBytesUsed() + INSTANCE_SIZE - AbstractDefaultAggTableScanOperator.INSTANCE_SIZE;
  }
}
