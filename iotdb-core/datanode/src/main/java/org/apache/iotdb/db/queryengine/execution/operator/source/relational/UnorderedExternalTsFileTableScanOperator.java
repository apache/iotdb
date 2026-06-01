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

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.EncryptDBUtils;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.calc.plan.planner.CommonOperatorUtils.CURRENT_DEVICE_INDEX_STRING;

public class UnorderedExternalTsFileTableScanOperator extends AbstractTableScanOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(UnorderedExternalTsFileTableScanOperator.class);

  private final String tableName;
  private final SchemaFilter deviceFilter;

  private MultiTsFileResourceIterator deviceIterator;
  private Map<TsFileResource, TsFileSequenceReader> resourceReaderMap = Collections.emptyMap();
  private DeviceEntry currentDeviceEntry;
  private int currentDeviceIndex;

  public UnorderedExternalTsFileTableScanOperator(
      AbstractTableScanOperatorParameter parameter, String tableName, SchemaFilter deviceFilter) {
    super(parameter);
    this.tableName = tableName;
    this.deviceFilter = deviceFilter;
    this.currentDeviceIndex = 0;
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
  public void initQueryDataSource(IQueryDataSource dataSource) {
    super.initQueryDataSource(dataSource);

    QueryDataSource queryDataSource = (QueryDataSource) dataSource;
    initDeviceIterator(queryDataSource);
    currentDeviceEntry = nextDeviceEntry();
    recordCurrentDeviceIndex();
    constructAlignedSeriesScanUtil();
    if (seriesScanUtil != null) {
      seriesScanUtil.initQueryDataSource(queryDataSource);
    }
  }

  private void initDeviceIterator(QueryDataSource queryDataSource) {
    resourceReaderMap = createResourceReaderMap(getAllResources(queryDataSource));
    deviceIterator =
        new MultiTsFileResourceIterator(
            tableName,
            queryDataSource.getSeqResources(),
            queryDataSource.getUnseqResources(),
            resourceReaderMap,
            ((OperatorContext) operatorContext).getInstanceContext(),
            seriesScanOptions,
            deviceFilter);
  }

  private Map<TsFileResource, TsFileSequenceReader> createResourceReaderMap(
      List<TsFileResource> resources) {
    Map<TsFileResource, TsFileSequenceReader> readerMap = new HashMap<>(resources.size());
    for (TsFileResource resource : resources) {
      try {
        readerMap.put(
            resource,
            new TsFileSequenceReader(
                resource.getTsFilePath(),
                ((OperatorContext) operatorContext)
                        .getInstanceContext()
                        .getQueryStatistics()
                        .getLoadTimeSeriesMetadataActualIOSize()
                    ::addAndGet,
                EncryptDBUtils.getFirstEncryptParamFromTSFilePath(resource.getTsFilePath())));
      } catch (IOException e) {
        closeResourceReaders(readerMap);
        throw new RuntimeException(
            "Failed to open external TsFile reader: " + resource.getTsFilePath(), e);
      }
    }
    return readerMap;
  }

  private List<TsFileResource> getAllResources(QueryDataSource queryDataSource) {
    List<TsFileResource> resources =
        new ArrayList<>(
            queryDataSource.getSeqResources().size() + queryDataSource.getUnseqResources().size());
    resources.addAll(queryDataSource.getSeqResources());
    resources.addAll(queryDataSource.getUnseqResources());
    return resources;
  }

  private DeviceEntry nextDeviceEntry() {
    if (deviceIterator == null || !deviceIterator.hasNextDevice()) {
      return null;
    }
    IDeviceID nextDevice = deviceIterator.nextDevice();
    return nextDevice == null ? null : new AlignedDeviceEntry(nextDevice, new Binary[0]);
  }

  @Override
  protected boolean hasCurrentDeviceEntry() {
    return currentDeviceEntry != null;
  }

  @Override
  protected DeviceEntry getCurrentDeviceEntry() {
    return currentDeviceEntry;
  }

  @Override
  protected boolean advanceDeviceEntry() {
    currentDeviceIndex++;
    currentDeviceEntry = nextDeviceEntry();
    return currentDeviceEntry != null;
  }

  @Override
  protected void recordCurrentDeviceIndex() {
    operatorContext.recordSpecifiedInfo(
        CURRENT_DEVICE_INDEX_STRING, Integer.toString(currentDeviceIndex));
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
            deviceIterator);
  }

  @Override
  public void close() throws Exception {
    closeResourceReaders(resourceReaderMap);
    resourceReaderMap = Collections.emptyMap();
    deviceIterator = null;
    super.close();
  }

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed()
        + INSTANCE_SIZE
        - AbstractTableScanOperator.INSTANCE_SIZE
        + RamUsageEstimator.sizeOfMap(resourceReaderMap);
  }

  private void closeResourceReaders(Map<TsFileResource, TsFileSequenceReader> readerMap) {
    for (TsFileSequenceReader reader : readerMap.values()) {
      try {
        reader.close();
      } catch (IOException ignored) {
        // ignore close failure
      }
    }
  }
}
