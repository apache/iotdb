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
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.TableScanNode;
import org.apache.iotdb.commons.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.OrderingScheme;
import org.apache.iotdb.commons.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.source.FileLoaderUtils;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.read.IQueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.AbstractAlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.LazyTsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.iotdb.calc.plan.planner.CommonOperatorUtils.CURRENT_DEVICE_INDEX_STRING;

public class OrderedExternalTsFileTableScanOperator extends AbstractTableScanOperator {
  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(OrderedExternalTsFileTableScanOperator.class);

  private final String tableName;
  private final Map<Symbol, ColumnSchema> assignments;
  private final OrderingScheme pushedOrderingScheme;
  private final SchemaFilter deviceFilter;
  private final ExternalTsFileDeviceFilterVisitor deviceFilterVisitor =
      new ExternalTsFileDeviceFilterVisitor();
  private final Map<TsFileResource, Map<IDeviceID, long[]>> deviceMeasurementNodeOffsetMap =
      new HashMap<>();

  private DeviceEntry currentDeviceEntry;
  private int currentDeviceIndex;
  private List<DeviceEntry> sortedDeviceEntries = new ArrayList<>();

  public OrderedExternalTsFileTableScanOperator(
      AbstractTableScanOperatorParameter parameter,
      String tableName,
      Map<Symbol, ColumnSchema> assignments,
      OrderingScheme pushedOrderingScheme,
      SchemaFilter deviceFilter) {
    super(parameter);
    this.tableName = tableName;
    this.assignments = assignments;
    this.pushedOrderingScheme = pushedOrderingScheme;
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
    sortedDeviceEntries = collectSortedDeviceEntries((QueryDataSource) dataSource);
    currentDeviceEntry = sortedDeviceEntries.isEmpty() ? null : sortedDeviceEntries.get(0);
    recordCurrentDeviceIndex();
    constructAlignedSeriesScanUtil();
    if (seriesScanUtil != null) {
      seriesScanUtil.initQueryDataSource((QueryDataSource) dataSource);
    }
  }

  private List<DeviceEntry> collectSortedDeviceEntries(QueryDataSource queryDataSource) {
    List<ExternalTsFileDeviceInfo> deviceInfos = collectDeviceInfos(queryDataSource);
    deviceInfos.sort(createDeviceInfoComparator());

    List<DeviceEntry> deviceEntries = new ArrayList<>(deviceInfos.size());
    Set<IDeviceID> visitedDevices = new LinkedHashSet<>();
    for (ExternalTsFileDeviceInfo deviceInfo : deviceInfos) {
      deviceMeasurementNodeOffsetMap
          .computeIfAbsent(deviceInfo.resource, ignored -> new HashMap<>())
          .put(deviceInfo.deviceID, deviceInfo.deviceMeasurementNodeOffset);
      if (visitedDevices.add(deviceInfo.deviceID)) {
        deviceEntries.add(new AlignedDeviceEntry(deviceInfo.deviceID, new Binary[0]));
      }
    }
    return deviceEntries;
  }

  private List<ExternalTsFileDeviceInfo> collectDeviceInfos(QueryDataSource queryDataSource) {
    List<ExternalTsFileDeviceInfo> deviceInfos = new ArrayList<>();
    for (TsFileResource resource : getAllResources(queryDataSource)) {
      collectDeviceInfos(resource, deviceInfos);
    }
    return deviceInfos;
  }

  private void collectDeviceInfos(
      TsFileResource resource, List<ExternalTsFileDeviceInfo> deviceInfos) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(resource.getTsFilePath())) {
      LazyTsFileDeviceIterator deviceIterator =
          new LazyTsFileDeviceIterator(
              reader,
              tableName,
              ((OperatorContext) operatorContext)
                      .getInstanceContext()
                      .getQueryStatistics()
                      .getLoadTimeSeriesMetadataActualIOSize()
                  ::addAndGet);
      while (deviceIterator.hasNext()) {
        IDeviceID deviceID = deviceIterator.next();
        if (!isDeviceMatched(deviceID)) {
          continue;
        }
        deviceInfos.add(
            new ExternalTsFileDeviceInfo(
                deviceID, resource, deviceIterator.getCurrentDeviceMeasurementNodeOffset()));
      }
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to collect devices from external TsFile: " + resource.getTsFilePath(), e);
    }
  }

  private List<TsFileResource> getAllResources(QueryDataSource queryDataSource) {
    List<TsFileResource> resources =
        new ArrayList<>(
            queryDataSource.getSeqResources().size() + queryDataSource.getUnseqResources().size());
    resources.addAll(queryDataSource.getSeqResources());
    resources.addAll(queryDataSource.getUnseqResources());
    return resources;
  }

  private boolean isDeviceMatched(IDeviceID deviceID) {
    return deviceFilter == null
        || Boolean.TRUE.equals(deviceFilter.accept(deviceFilterVisitor, deviceID));
  }

  private Comparator<ExternalTsFileDeviceInfo> createDeviceInfoComparator() {
    Comparator<ExternalTsFileDeviceInfo> comparator = null;
    for (Symbol symbol : pushedOrderingScheme.getOrderBy()) {
      if (TableScanNode.isTimeColumn(symbol, assignments)) {
        continue;
      }
      int tagIndex = getTagIndex(symbol);
      final int deviceSegmentIndex = tagIndex + 1;
      Comparator<String> valueComparator =
          pushedOrderingScheme.getOrdering(symbol).isNullsFirst()
              ? Comparator.nullsFirst(Comparator.naturalOrder())
              : Comparator.nullsLast(Comparator.naturalOrder());
      Comparator<ExternalTsFileDeviceInfo> currentComparator =
          Comparator.comparing(
              deviceInfo -> getDeviceSegment(deviceInfo.deviceID, deviceSegmentIndex),
              valueComparator);
      if (!pushedOrderingScheme.getOrdering(symbol).isAscending()) {
        currentComparator = currentComparator.reversed();
      }
      comparator =
          comparator == null ? currentComparator : comparator.thenComparing(currentComparator);
    }
    return comparator == null
        ? Comparator.comparing(deviceInfo -> deviceInfo.deviceID)
        : comparator.thenComparing(deviceInfo -> deviceInfo.deviceID);
  }

  private String getDeviceSegment(IDeviceID deviceID, int deviceSegmentIndex) {
    return deviceSegmentIndex < deviceID.segmentNum()
        ? (String) deviceID.segment(deviceSegmentIndex)
        : null;
  }

  private int getTagIndex(Symbol symbol) {
    int tagIndex = 0;
    for (Map.Entry<Symbol, ColumnSchema> entry : assignments.entrySet()) {
      if (entry.getValue().getColumnCategory() != TsTableColumnCategory.TAG) {
        continue;
      }
      if (entry.getKey().equals(symbol)) {
        return tagIndex;
      }
      tagIndex++;
    }
    throw new IllegalArgumentException("Unexpected external TsFile ordering symbol: " + symbol);
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
    currentDeviceEntry =
        currentDeviceIndex < sortedDeviceEntries.size()
            ? sortedDeviceEntries.get(currentDeviceIndex)
            : null;
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
            this::loadTimeSeriesMetadata);
  }

  private AbstractAlignedTimeSeriesMetadata loadTimeSeriesMetadata(
      TsFileResource resource, AlignedFullPath alignedPath) throws IOException {
    Optional<long[]> deviceMeasurementNodeOffset =
        Optional.ofNullable(deviceMeasurementNodeOffsetMap)
            .map(map -> map.get(resource))
            .map(map -> map.get(alignedPath.getDeviceId()));
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

  @Override
  public long ramBytesUsed() {
    return super.ramBytesUsed()
        + INSTANCE_SIZE
        - AbstractTableScanOperator.INSTANCE_SIZE
        + RamUsageEstimator.sizeOfMap(deviceMeasurementNodeOffsetMap)
        + RamUsageEstimator.sizeOfCollection(sortedDeviceEntries);
  }

  private static class ExternalTsFileDeviceInfo {
    private final IDeviceID deviceID;
    private final TsFileResource resource;
    private final long[] deviceMeasurementNodeOffset;

    private ExternalTsFileDeviceInfo(
        IDeviceID deviceID, TsFileResource resource, long[] deviceMeasurementNodeOffset) {
      this.deviceID = deviceID;
      this.resource = resource;
      this.deviceMeasurementNodeOffset = deviceMeasurementNodeOffset;
    }
  }
}
