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

package org.apache.iotdb.db.storageengine.dataregion.utils;

import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileID;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.TableDiskUsageCache;
import org.apache.iotdb.db.storageengine.dataregion.utils.tableDiskUsageCache.TimePartitionTableSizeQueryContext;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;

public class TableDiskUsageStatisticUtil extends DiskUsageStatisticUtil {
  public static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableDiskUsageStatisticUtil.class);
  private final String database;
  private final List<Pair<TsFileID, Long>> tsFilesToQueryInCache;
  private final TimePartitionTableSizeQueryContext tableSizeQueryContext;
  private final boolean databaseHasOnlyOneTable;
  private final boolean needAllData;

  public TableDiskUsageStatisticUtil(
      DataRegion dataRegion,
      long timePartition,
      TimePartitionTableSizeQueryContext tableSizeQueryContext,
      boolean needAllData,
      boolean databaseHasOnlyOneTable,
      List<Pair<TsFileID, Long>> tsFilesToQueryInCache,
      Optional<FragmentInstanceContext> context) {
    super(dataRegion.getTsFileManager(), timePartition, context);
    this.database = dataRegion.getDatabaseName();
    this.tableSizeQueryContext = tableSizeQueryContext;
    this.tsFilesToQueryInCache = tsFilesToQueryInCache;
    this.databaseHasOnlyOneTable = databaseHasOnlyOneTable;
    this.needAllData = needAllData;
  }

  @Override
  protected boolean calculateWithoutOpenFile(TsFileResource tsFileResource) {
    TsFileID tsFileID = tsFileResource.getTsFileID();
    Long cachedValueOffset = tableSizeQueryContext.getCachedTsFileIdOffset(tsFileID);
    if (cachedValueOffset != null) {
      tsFilesToQueryInCache.add(new Pair<>(tsFileID, cachedValueOffset));
      return true;
    }

    if (!databaseHasOnlyOneTable || tsFileResource.anyModFileExists()) {
      return false;
    }
    String table = tableSizeQueryContext.getTableSizeResultMap().keySet().iterator().next();
    tableSizeQueryContext.updateResult(table, tsFileResource.getTsFileSize(), needAllData);
    TableDiskUsageCache.getInstance()
        .write(
            database,
            tsFileResource.getTsFileID(),
            Collections.singletonMap(table, tsFileResource.getTsFileSize()));
    return true;
  }

  @Override
  protected void calculateNextFile(TsFileResource tsFileResource, TsFileSequenceReader reader)
      throws IOException {
    TsFileMetadata tsFileMetadata = reader.readFileMetadata();
    if (!needAllData && !hasSatisfiedData(tsFileMetadata)) {
      return;
    }

    if (tsFileMetadata.getTableMetadataIndexNodeMap().size() == 1) {
      String satisfiedTable =
          tsFileMetadata.getTableMetadataIndexNodeMap().keySet().iterator().next();
      tableSizeQueryContext.updateResult(
          satisfiedTable, tsFileResource.getTsFileSize(), needAllData);
      TableDiskUsageCache.getInstance()
          .write(
              database,
              tsFileResource.getTsFileID(),
              Collections.singletonMap(
                  tsFileMetadata.getTableMetadataIndexNodeMap().keySet().iterator().next(),
                  tsFileResource.getTsFileSize()));
      return;
    }

    calculateDiskUsageInBytesByOffset(tsFileResource, reader);
  }

  private boolean hasSatisfiedData(TsFileMetadata tsFileMetadata) {
    Map<String, MetadataIndexNode> tableMetadataIndexNodeMap =
        tsFileMetadata.getTableMetadataIndexNodeMap();
    return tableSizeQueryContext.getTableSizeResultMap().keySet().stream()
        .anyMatch(tableMetadataIndexNodeMap::containsKey);
  }

  private void calculateDiskUsageInBytesByOffset(
      TsFileResource resource, TsFileSequenceReader reader) throws IOException {
    Map<String, Long> tsFileTableSizeMap =
        calculateTableSizeMap(
            reader, timeSeriesMetadataCountRecorder, timeSeriesMetadataIoSizeRecorder);
    for (Map.Entry<String, Long> entry : tsFileTableSizeMap.entrySet()) {
      tableSizeQueryContext.updateResult(entry.getKey(), entry.getValue(), needAllData);
    }
    TableDiskUsageCache.getInstance().write(database, resource.getTsFileID(), tsFileTableSizeMap);
  }

  public static Optional<Map<String, Long>> calculateTableSizeMap(TsFileResource resource) {
    if (!resource.getTsFile().exists()) {
      return Optional.empty();
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(resource.getTsFilePath())) {
      return Optional.of(calculateTableSizeMap(reader, null, null));
    } catch (Exception e) {
      logger.error("Failed to calculate tsfile table sizes", e);
      return Optional.empty();
    }
  }

  public static Map<String, Long> calculateTableSizeMap(
      TsFileSequenceReader reader,
      @Nullable LongConsumer timeSeriesMetadataCountRecorder,
      @Nullable LongConsumer timeSeriesMetadataIoSizeRecorder)
      throws IOException {
    TsFileMetadata tsFileMetadata = reader.readFileMetadata();
    Map<String, MetadataIndexNode> tableMetadataIndexNodeMap =
        tsFileMetadata.getTableMetadataIndexNodeMap();
    String currentTable = null, nextTable = null;
    Iterator<String> iterator = tableMetadataIndexNodeMap.keySet().iterator();
    Map<String, Offsets> tableOffsetMap = new HashMap<>();
    Map<String, Long> tsFileTableSizeMap = new HashMap<>();
    while (currentTable != null || iterator.hasNext()) {
      currentTable = currentTable == null ? iterator.next() : currentTable;
      nextTable = iterator.hasNext() ? iterator.next() : null;
      long tableSize =
          calculateTableSize(
              tableOffsetMap,
              tsFileMetadata,
              reader,
              currentTable,
              nextTable,
              timeSeriesMetadataCountRecorder,
              timeSeriesMetadataIoSizeRecorder);
      tsFileTableSizeMap.put(currentTable, tableSize);
      currentTable = nextTable;
    }
    return tsFileTableSizeMap;
  }

  private static long calculateTableSize(
      Map<String, Offsets> tableOffsetMap,
      TsFileMetadata tsFileMetadata,
      TsFileSequenceReader reader,
      String tableName,
      String nextTable,
      LongConsumer timeSeriesMetadataCountRecorder,
      LongConsumer timeSeriesMetadataIoSizeRecorder)
      throws IOException {
    Offsets startOffset =
        getTableOffset(
            tableOffsetMap,
            reader,
            tableName,
            timeSeriesMetadataCountRecorder,
            timeSeriesMetadataIoSizeRecorder);
    Offsets endOffset;
    if (nextTable == null) {
      long firstMeasurementNodeOffsetOfFirstTable;
      String firstTableName =
          tsFileMetadata.getTableMetadataIndexNodeMap().keySet().iterator().next();
      Offsets firstTableOffset = tableOffsetMap.get(firstTableName);
      if (firstTableOffset != null) {
        firstMeasurementNodeOffsetOfFirstTable = firstTableOffset.firstMeasurementNodeOffset;
      } else {
        TsFileDeviceIterator deviceIterator =
            reader.getTableDevicesIteratorWithIsAligned(
                tableName, timeSeriesMetadataIoSizeRecorder);
        deviceIterator.next();
        firstMeasurementNodeOffsetOfFirstTable =
            deviceIterator.getCurrentDeviceMeasurementNodeOffset()[0];
      }
      endOffset =
          new Offsets(
              tsFileMetadata.getMetaOffset(),
              firstMeasurementNodeOffsetOfFirstTable,
              reader.getFileMetadataPos());
    } else {
      endOffset =
          getTableOffset(
              tableOffsetMap,
              reader,
              nextTable,
              timeSeriesMetadataCountRecorder,
              timeSeriesMetadataIoSizeRecorder);
    }
    return endOffset.minusOffsetForTableModel(startOffset);
  }

  private static Offsets getTableOffset(
      Map<String, Offsets> tableOffsetMap,
      TsFileSequenceReader reader,
      String tableName,
      LongConsumer timeSeriesMetadataCountRecorder,
      LongConsumer timeSeriesMetadataIoSizeRecorder) {
    return tableOffsetMap.computeIfAbsent(
        tableName,
        k -> {
          try {
            TsFileDeviceIterator deviceIterator =
                reader.getTableDevicesIteratorWithIsAligned(k, timeSeriesMetadataIoSizeRecorder);
            Pair<IDeviceID, Boolean> pair = deviceIterator.next();
            return calculateStartOffsetOfChunkGroupAndTimeseriesMetadata(
                reader,
                deviceIterator.getFirstMeasurementNodeOfCurrentDevice(),
                pair,
                deviceIterator.getCurrentDeviceMeasurementNodeOffset()[0],
                timeSeriesMetadataCountRecorder,
                timeSeriesMetadataIoSizeRecorder);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
