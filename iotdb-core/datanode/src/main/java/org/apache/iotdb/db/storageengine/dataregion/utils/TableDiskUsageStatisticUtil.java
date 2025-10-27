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

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.conf.TSFileConfig;
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

public class TableDiskUsageStatisticUtil extends DiskUsageStatisticUtil {
  public static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TableDiskUsageStatisticUtil.class);
  private final Map<String, Integer> tableIndexMap;
  private final long[] resultArr;

  public TableDiskUsageStatisticUtil(
      TsFileManager tsFileManager, long timePartition, List<String> tableNames) {
    super(tsFileManager, timePartition);
    this.tableIndexMap = new HashMap<>();
    for (int i = 0; i < tableNames.size(); i++) {
      tableIndexMap.put(tableNames.get(i), i);
    }
    this.resultArr = new long[tableNames.size()];
  }

  @Override
  public long[] getResult() {
    return resultArr;
  }

  @Override
  public void calculateNextFile() {
    TsFileResource tsFileResource = iterator.next();
    if (tsFileResource.isDeleted()) {
      return;
    }
    try (TsFileSequenceReader reader = new TsFileSequenceReader(tsFileResource.getTsFilePath())) {
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      if (!hasSatisfiedData(tsFileMetadata)) {
        return;
      }
      int allSatisfiedTableIndex = getAllSatisfiedTableIndex(tsFileMetadata);
      if (allSatisfiedTableIndex > 0) {
        // size of tsfile - size of (tsfile magic string + version number + all metadata + metadata
        // marker)
        resultArr[allSatisfiedTableIndex] +=
            (tsFileResource.getTsFileSize()
                - reader.getAllMetadataSize()
                - 1
                - TSFileConfig.MAGIC_STRING.getBytes().length
                - 1);
        return;
      }
      calculateDiskUsageInBytesByOffset(reader);
    } catch (Exception e) {
      logger.error("Failed to scan file {}", tsFileResource.getTsFile().getAbsolutePath(), e);
    }
  }

  private boolean hasSatisfiedData(TsFileMetadata tsFileMetadata) {
    Map<String, MetadataIndexNode> tableMetadataIndexNodeMap =
        tsFileMetadata.getTableMetadataIndexNodeMap();
    return tableIndexMap.keySet().stream().anyMatch(tableMetadataIndexNodeMap::containsKey);
  }

  private int getAllSatisfiedTableIndex(TsFileMetadata tsFileMetadata) {
    if (tsFileMetadata.getTableMetadataIndexNodeMap().size() != 1) {
      return -1;
    }
    String satisfiedTableName =
        tsFileMetadata.getTableMetadataIndexNodeMap().keySet().iterator().next();
    return tableIndexMap.get(satisfiedTableName);
  }

  private void calculateDiskUsageInBytesByOffset(TsFileSequenceReader reader) throws IOException {
    TsFileMetadata tsFileMetadata = reader.readFileMetadata();
    Map<String, MetadataIndexNode> tableMetadataIndexNodeMap =
        tsFileMetadata.getTableMetadataIndexNodeMap();
    String nextTable = null;
    Iterator<String> iterator = tableMetadataIndexNodeMap.keySet().iterator();
    Map<String, Long> tableOffsetMap = new HashMap<>();
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
      Map<String, Long> tableOffsetMap,
      TsFileMetadata tsFileMetadata,
      TsFileSequenceReader reader,
      String tableName,
      String nextTable) {
    long startOffset, endOffset;
    if (nextTable == null) {
      endOffset = tsFileMetadata.getMetaOffset();
    } else {
      endOffset = getTableOffset(tableOffsetMap, reader, nextTable);
    }
    startOffset = getTableOffset(tableOffsetMap, reader, tableName);
    return endOffset - startOffset;
  }

  private long getTableOffset(
      Map<String, Long> tableOffsetMap, TsFileSequenceReader reader, String tableName) {
    return tableOffsetMap.computeIfAbsent(
        tableName,
        k -> {
          try {
            TsFileDeviceIterator deviceIterator = reader.getTableDevicesIteratorWithIsAligned(k);
            Pair<IDeviceID, Boolean> pair = deviceIterator.next();
            return calculateStartOffsetOfChunkGroup(
                reader, deviceIterator.getFirstMeasurementNodeOfCurrentDevice(), pair);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        });
  }
}
