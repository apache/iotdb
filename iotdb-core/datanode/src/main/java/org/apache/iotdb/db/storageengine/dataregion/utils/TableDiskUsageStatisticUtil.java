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
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.read.TsFileDeviceIterator;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TableDiskUsageStatisticUtil extends DiskUsageStatisticUtil {
  public static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableDiskUsageStatisticUtil.class);
  private final Map<String, Integer> tableIndexMap;
  private final boolean databaseHasOnlyOneTable;
  private final long[] resultArr;

  public TableDiskUsageStatisticUtil(
      TsFileManager tsFileManager,
      long timePartition,
      List<String> tableNames,
      boolean databaseHasOnlyOneTable,
      Optional<FragmentInstanceContext> context) {
    super(tsFileManager, timePartition, context);
    this.tableIndexMap = new HashMap<>();
    for (int i = 0; i < tableNames.size(); i++) {
      tableIndexMap.put(tableNames.get(i), i);
    }
    this.databaseHasOnlyOneTable = databaseHasOnlyOneTable;
    this.resultArr = new long[tableNames.size()];
  }

  @Override
  public long[] getResult() {
    return resultArr;
  }

  @Override
  protected boolean calculateWithoutOpenFile(TsFileResource tsFileResource) {
    if (!databaseHasOnlyOneTable || tsFileResource.anyModFileExists()) {
      return false;
    }
    resultArr[0] += tsFileResource.getTsFileSize();
    return true;
  }

  @Override
  protected void calculateNextFile(TsFileResource tsFileResource, TsFileSequenceReader reader)
      throws IOException {
    TsFileMetadata tsFileMetadata = reader.readFileMetadata();
    if (!hasSatisfiedData(tsFileMetadata)) {
      return;
    }
    Pair<Integer, Boolean> allSatisfiedTableIndexPair = getAllSatisfiedTableIndex(tsFileMetadata);
    int allSatisfiedTableIndex = allSatisfiedTableIndexPair.getLeft();
    // the only one table in this tsfile might be deleted by mods, and it is not the table we
    // queried
    boolean mayContainSearchedTable = allSatisfiedTableIndexPair.getRight();
    if (allSatisfiedTableIndex != -1) {
      resultArr[allSatisfiedTableIndex] +=
          mayContainSearchedTable ? tsFileResource.getTsFileSize() : 0;
      return;
    }
    calculateDiskUsageInBytesByOffset(reader);
  }

  private boolean hasSatisfiedData(TsFileMetadata tsFileMetadata) {
    Map<String, MetadataIndexNode> tableMetadataIndexNodeMap =
        tsFileMetadata.getTableMetadataIndexNodeMap();
    return tableIndexMap.keySet().stream().anyMatch(tableMetadataIndexNodeMap::containsKey);
  }

  private Pair<Integer, Boolean> getAllSatisfiedTableIndex(TsFileMetadata tsFileMetadata) {
    if (tsFileMetadata.getTableMetadataIndexNodeMap().size() != 1) {
      return new Pair<>(-1, true);
    }
    String satisfiedTableName =
        tsFileMetadata.getTableMetadataIndexNodeMap().keySet().iterator().next();
    String searchedTableName = tableIndexMap.keySet().iterator().next();
    return new Pair<>(
        tableIndexMap.get(satisfiedTableName), satisfiedTableName.equals(searchedTableName));
  }

  private void calculateDiskUsageInBytesByOffset(TsFileSequenceReader reader) throws IOException {
    TsFileMetadata tsFileMetadata = reader.readFileMetadata();
    Map<String, MetadataIndexNode> tableMetadataIndexNodeMap =
        tsFileMetadata.getTableMetadataIndexNodeMap();
    String nextTable = null;
    Iterator<String> iterator = tableMetadataIndexNodeMap.keySet().iterator();
    Map<String, Offsets> tableOffsetMap = new HashMap<>();
    while (iterator.hasNext()) {
      String currentTable = iterator.next();
      while (currentTable != null && tableIndexMap.containsKey(currentTable)) {
        nextTable = iterator.hasNext() ? iterator.next() : null;
        long tableSize =
            calculateTableSize(tableOffsetMap, tsFileMetadata, reader, currentTable, nextTable);
        resultArr[tableIndexMap.get(currentTable)] += tableSize;
        currentTable = nextTable;
      }
    }
  }

  private long calculateTableSize(
      Map<String, Offsets> tableOffsetMap,
      TsFileMetadata tsFileMetadata,
      TsFileSequenceReader reader,
      String tableName,
      String nextTable)
      throws IOException {
    Offsets startOffset = getTableOffset(tableOffsetMap, reader, tableName);
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
      endOffset = getTableOffset(tableOffsetMap, reader, nextTable);
    }
    return endOffset.minusOffsetForTableModel(startOffset);
  }

  private Offsets getTableOffset(
      Map<String, Offsets> tableOffsetMap, TsFileSequenceReader reader, String tableName) {
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
                deviceIterator.getCurrentDeviceMeasurementNodeOffset()[0]);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
