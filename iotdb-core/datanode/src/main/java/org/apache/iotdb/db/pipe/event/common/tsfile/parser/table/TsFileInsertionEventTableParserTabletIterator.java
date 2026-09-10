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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser.table;

import org.apache.iotdb.commons.exception.pipe.PipeRuntimeOutOfMemoryCriticalException;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.db.exception.load.LoadRuntimeOutOfMemoryException;
import org.apache.iotdb.db.i18n.DataNodePipeMessages;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeTabletUtils.TabletStringInternPool;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParserMemoryBlock;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.util.ModsOperationUtil;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.utils.datastructure.PatternTreeMapFactory;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.controller.IMetadataQuerier;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.reader.IChunkReader;
import org.apache.tsfile.read.reader.chunk.TableChunkReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

public class TsFileInsertionEventTableParserTabletIterator implements Iterator<Tablet> {

  private final long startTime;
  private final long endTime;

  // Used to read or record TSFileMeta tools or meta information
  private final TsFileSequenceReader reader;
  private final IMetadataQuerier metadataQuerier;
  private final TsFileMetadata fileMetadata;
  private final Iterator<Map.Entry<String, TableSchema>> filteredTableSchemaIterator;
  private final TabletStringInternPool tabletStringInternPool = new TabletStringInternPool();

  // For memory control
  private final TsFileInsertionEventParserMemoryBlock allocatedMemoryBlockForTablet;
  private final TsFileInsertionEventParserMemoryBlock allocatedMemoryBlockForBatchData;
  private final TsFileInsertionEventParserMemoryBlock allocatedMemoryBlockForChunk;
  private final TsFileInsertionEventParserMemoryBlock allocatedMemoryBlockForChunkMeta;
  private final TsFileInsertionEventParserMemoryBlock allocatedMemoryBlockForTableSchema;

  // mods entry
  private final PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications;

  // Used to read tsfile data
  private IChunkReader chunkReader;
  private BatchData batchData;
  private Tablet pendingTabletAfterMemoryPressure;

  // Record the metadata information of the currently read Table
  private Iterator<Pair<IDeviceID, MetadataIndexNode>> deviceMetaIterator;
  private Iterator<AbstractAlignedChunkMetadata> chunkMetadataList;
  private Iterator<IChunkMetadata> chunkMetadata;
  private IDeviceID pendingDeviceID;
  private List<AbstractAlignedChunkMetadata> pendingAlignedChunkMetadataList;
  private AbstractAlignedChunkMetadata currentChunkMetadata;
  private Chunk timeChunk;
  private long timeChunkSize;
  private int offset;

  // Record the information of the currently read Table
  private String tableName;
  private IDeviceID deviceID;
  private List<ColumnCategory> columnTypes;
  private List<String> measurementList;
  private List<TSDataType> dataTypeList;
  private List<IMeasurementSchema> fieldSchemaList;
  private int deviceIdSize;

  private List<ModsOperationUtil.ModsInfo> modsInfoList;

  // Used to record whether the same Tablet is generated when parsing starts. Different table
  // information cannot be placed in the same Tablet.
  private boolean isSameTableName;
  private boolean isSameDeviceID;

  public TsFileInsertionEventTableParserTabletIterator(
      final TsFileSequenceReader tsFileSequenceReader,
      final Predicate<Map.Entry<String, TableSchema>> predicate,
      final TsFileInsertionEventParserMemoryBlock allocatedMemoryBlockForTablet,
      final TsFileInsertionEventParserMemoryBlock allocatedMemoryBlockForBatchData,
      final TsFileInsertionEventParserMemoryBlock allocatedMemoryBlockForChunk,
      final TsFileInsertionEventParserMemoryBlock allocatedMemoryBlockForChunkMeta,
      final TsFileInsertionEventParserMemoryBlock allocatedMemoryBlockForTableSchema,
      final PatternTreeMap<ModEntry, PatternTreeMapFactory.ModsSerializer> modifications,
      final long startTime,
      final long endTime)
      throws IOException {

    this.startTime = startTime;
    this.endTime = endTime;
    this.modifications = modifications;

    this.reader = tsFileSequenceReader;
    this.metadataQuerier = new MetadataQuerierByFileImpl(reader);
    fileMetadata = this.metadataQuerier.getWholeFileMetadata();
    final List<Map.Entry<String, TableSchema>> tableSchemaList =
        new ArrayList<>(fileMetadata.getTableSchemaMap().size());
    for (final Map.Entry<String, TableSchema> entry : fileMetadata.getTableSchemaMap().entrySet()) {
      if (predicate.test(entry)) {
        tableSchemaList.add(entry);
      }
    }

    this.allocatedMemoryBlockForTablet = allocatedMemoryBlockForTablet;
    this.allocatedMemoryBlockForBatchData = allocatedMemoryBlockForBatchData;
    this.allocatedMemoryBlockForChunk = allocatedMemoryBlockForChunk;
    this.allocatedMemoryBlockForChunkMeta = allocatedMemoryBlockForChunkMeta;
    this.allocatedMemoryBlockForTableSchema = allocatedMemoryBlockForTableSchema;

    long tableSchemaSize = fileMetadata.getBloomFilter().getRetainedSizeInBytes();
    for (Map.Entry<String, TableSchema> tableSchemaEntry : tableSchemaList) {
      tableSchemaSize +=
          tableSchemaEntry.getKey().length()
              + PipeMemoryWeightUtil.calculateTableSchemaBytesUsed(tableSchemaEntry.getValue());
      if (tableSchemaSize > allocatedMemoryBlockForTableSchema.getMemoryUsageInBytes()) {
        this.allocatedMemoryBlockForTableSchema.forceResize(tableSchemaSize);
      }
    }

    filteredTableSchemaIterator = tableSchemaList.iterator();
  }

  @Override
  public boolean hasNext() {
    try {
      State state = State.CHECK_DATA;
      while (true) {
        switch (state) {
          case CHECK_DATA:
            if (batchData != null && batchData.hasCurrent()) {
              return true;
            }
          case INIT_DATA:
            if (chunkReader != null && chunkReader.hasNextSatisfiedPage()) {
              batchData = chunkReader.nextPageData();
              final long size = PipeMemoryWeightUtil.calculateBatchDataRamBytesUsed(batchData);
              if (allocatedMemoryBlockForBatchData.getMemoryUsageInBytes() < size) {
                allocatedMemoryBlockForBatchData.forceResize(size);
              }
              state = State.CHECK_DATA;
              break;
            }
          case INIT_CHUNK_READER:
            if (currentChunkMetadata != null
                || (chunkMetadataList != null && chunkMetadataList.hasNext())) {
              if (currentChunkMetadata == null) {
                currentChunkMetadata = chunkMetadataList.next();
                timeChunk = null;
                offset = 0;
              }
              initChunkReader(currentChunkMetadata);
              state = State.INIT_DATA;
              break;
            }
          case INIT_CHUNK_METADATA:
            if (pendingDeviceID != null
                || deviceMetaIterator != null && deviceMetaIterator.hasNext()) {
              if (pendingDeviceID == null) {
                pendingDeviceID = deviceMetaIterator.next().getLeft();
              }
              if (pendingAlignedChunkMetadataList == null) {
                pendingAlignedChunkMetadataList =
                    reader.getAlignedChunkMetadata(pendingDeviceID, false);
              }
              long size = 0;
              final Iterator<AbstractAlignedChunkMetadata> chunkMetadataIterator =
                  pendingAlignedChunkMetadataList.iterator();
              while (chunkMetadataIterator.hasNext()) {
                final AbstractAlignedChunkMetadata alignedChunkMetadata =
                    chunkMetadataIterator.next();
                if (alignedChunkMetadata == null) {
                  throw new PipeException(
                      DataNodePipeMessages.TABLE_MODEL_TSFILE_PARSING_DOES_NOT_SUPPORT);
                }

                // Reduce the number of times Chunks are read
                if (alignedChunkMetadata.getEndTime() < startTime
                    || alignedChunkMetadata.getStartTime() > endTime) {
                  chunkMetadataIterator.remove();
                  continue;
                }

                if (areAllFieldsDeletedByMods(pendingDeviceID, alignedChunkMetadata)) {
                  chunkMetadataIterator.remove();
                  continue;
                }

                size +=
                    PipeMemoryWeightUtil.calculateAlignedChunkMetaBytesUsed(alignedChunkMetadata);
              }
              if (allocatedMemoryBlockForChunkMeta.getMemoryUsageInBytes() < size) {
                allocatedMemoryBlockForChunkMeta.forceResize(size);
              }

              deviceID = pendingDeviceID;
              chunkMetadataList = pendingAlignedChunkMetadataList.iterator();
              pendingDeviceID = null;
              pendingAlignedChunkMetadataList = null;

              state = State.INIT_CHUNK_READER;
              break;
            }
          case INIT_DEVICE_META:
            if (filteredTableSchemaIterator != null && filteredTableSchemaIterator.hasNext()) {
              final Map.Entry<String, TableSchema> entry = filteredTableSchemaIterator.next();
              tableName = tabletStringInternPool.intern(entry.getKey());
              final TableSchema tableSchema = entry.getValue();
              // The table name has changed, set to false
              isSameTableName = false;

              final MetadataIndexNode tableRoot = fileMetadata.getTableMetadataIndexNode(tableName);
              deviceMetaIterator = metadataQuerier.deviceIterator(tableRoot, null);

              final int columnSchemaSize = tableSchema.getColumnSchemas().size();
              dataTypeList = new ArrayList<>(columnSchemaSize);
              columnTypes = new ArrayList<>(columnSchemaSize);
              measurementList = new ArrayList<>(columnSchemaSize);
              fieldSchemaList = new ArrayList<>(columnSchemaSize);

              for (int i = 0; i < columnSchemaSize; i++) {
                final IMeasurementSchema schema = tableSchema.getColumnSchemas().get(i);
                final ColumnCategory columnCategory = tableSchema.getColumnTypes().get(i);
                if (schema != null
                    && schema.getMeasurementName() != null
                    && !schema.getMeasurementName().isEmpty()) {
                  final String measurementName = internMeasurementName(schema);
                  if (ColumnCategory.TAG.equals(columnCategory)) {
                    columnTypes.add(ColumnCategory.TAG);
                    measurementList.add(measurementName);
                    dataTypeList.add(schema.getType());
                  }
                  if (ColumnCategory.FIELD.equals(columnCategory)) {
                    fieldSchemaList.add(schema);
                  }
                }
              }
              deviceIdSize = dataTypeList.size();
              state = State.INIT_CHUNK_METADATA;
              break;
            }
            return false;
        }
      }
    } catch (Exception e) {
      throw new PipeException(e.getMessage(), e);
    }
  }

  private enum State {
    CHECK_DATA,
    INIT_DATA,
    INIT_CHUNK_READER,
    INIT_CHUNK_METADATA,
    INIT_DEVICE_META
  }

  @Override
  public Tablet next() {
    return buildNextTablet();
  }

  private Tablet buildNextTablet() {
    Tablet tablet = pendingTabletAfterMemoryPressure;
    pendingTabletAfterMemoryPressure = null;

    boolean isFirstRow = tablet == null;
    while (hasNext() && (isFirstRow || (isSameTableName && isSameDeviceID))) {
      if (batchData.currentTime() >= startTime && batchData.currentTime() <= endTime) {
        if (isFirstRow) {
          // Record the name of the table when the tablet is started. Different table data cannot be
          // in the same tablet.
          isSameTableName = true;
          isSameDeviceID = true;

          // Calculate row count and memory size of the tablet based on the first row
          final Pair<Integer, Integer> rowCountAndMemorySize =
              PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(batchData);
          if (allocatedMemoryBlockForTablet.getMemoryUsageInBytes()
              < rowCountAndMemorySize.getRight()) {
            allocatedMemoryBlockForTablet.forceResize(rowCountAndMemorySize.getRight());
          }

          tablet =
              new Tablet(
                  tableName,
                  new ArrayList<>(measurementList),
                  new ArrayList<>(dataTypeList),
                  new ArrayList<>(columnTypes),
                  rowCountAndMemorySize.getLeft());
          pendingTabletAfterMemoryPressure = tablet;
          isFirstRow = false;
        }
        final int rowIndex = tablet.getRowSize();
        if (rowIndex >= tablet.getMaxRowNumber()) {
          break;
        }

        if (fillMeasurementValueColumns(batchData, tablet, rowIndex)) {
          fillDeviceIdColumns(deviceID, tablet, rowIndex);
          PipeTabletUtils.putTimestamp(tablet, rowIndex, batchData.currentTime());
        }
      }

      if (batchData != null) {
        batchData.next();
      }
    }

    if (isFirstRow) {
      tablet = new Tablet(tableName, measurementList, dataTypeList, columnTypes, 0);
    }

    PipeTabletUtils.compactBitMaps(tablet);
    pendingTabletAfterMemoryPressure = null;
    return tablet;
  }

  private void initChunkReader(final AbstractAlignedChunkMetadata alignedChunkMetadata)
      throws IOException {
    if (Objects.isNull(timeChunk)) {
      timeChunk = reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
      timeChunkSize = PipeMemoryWeightUtil.calculateChunkRamBytesUsed(timeChunk);
      if (allocatedMemoryBlockForChunk.getMemoryUsageInBytes() < timeChunkSize) {
        allocatedMemoryBlockForChunk.forceResize(timeChunkSize);
      }
    }
    timeChunk.getData().rewind();
    long size = timeChunkSize;

    final int fieldSchemaSize = fieldSchemaList.size();
    final List<Chunk> valueChunkList = new ArrayList<>(fieldSchemaSize);
    final Map<String, IChunkMetadata> valueChunkMetadataMap =
        new HashMap<>((int) (fieldSchemaSize / 0.75f) + 1);
    for (final IChunkMetadata metadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      if (metadata != null
          && !isFieldDeletedByMods(
              metadata.getMeasurementUid(),
              alignedChunkMetadata.getStartTime(),
              alignedChunkMetadata.getEndTime())) {
        // Keep the first metadata entry to preserve the former merge-function behavior.
        valueChunkMetadataMap.putIfAbsent(metadata.getMeasurementUid(), metadata);
      }
    }

    // To ensure that the Tablet has the same alignedChunk column as the current one,
    // you need to create a new Tablet to fill in the data.
    isSameDeviceID = false;

    // Need to ensure that columnTypes recreates an array
    final List<ColumnCategory> categories = new ArrayList<>(deviceIdSize + fieldSchemaSize);
    for (int i = 0; i < deviceIdSize; i++) {
      categories.add(ColumnCategory.TAG);
    }
    columnTypes = categories;

    // Clean up the remaining non-DeviceID column information
    measurementList.subList(deviceIdSize, measurementList.size()).clear();
    dataTypeList.subList(deviceIdSize, dataTypeList.size()).clear();

    final int initialOffset = offset;
    boolean hasSelectedField = fieldSchemaList.isEmpty();
    boolean hasSelectedNonNullChunk = false;
    try {
      for (; offset < fieldSchemaList.size(); ++offset) {
        final IMeasurementSchema schema = fieldSchemaList.get(offset);
        final String measurementName = internMeasurementName(schema);
        if (isFieldDeletedByMods(
            measurementName,
            alignedChunkMetadata.getStartTime(),
            alignedChunkMetadata.getEndTime())) {
          continue;
        }

        final IChunkMetadata metadata = valueChunkMetadataMap.get(measurementName);
        Chunk chunk = null;
        if (metadata != null) {
          chunk = reader.readMemChunk((ChunkMetadata) metadata);
          final long newSize = size + PipeMemoryWeightUtil.calculateChunkRamBytesUsed(chunk);
          if (newSize > allocatedMemoryBlockForChunk.getMemoryUsageInBytes()) {
            if (!hasSelectedNonNullChunk) {
              // If the first chunk exceeds the memory limit, we need to allocate more memory
              size = newSize;
              allocatedMemoryBlockForChunk.forceResize(size);
            } else {
              break;
            }
          } else {
            size = newSize;
          }
          hasSelectedNonNullChunk = true;
        }
        columnTypes.add(ColumnCategory.FIELD);
        measurementList.add(measurementName);
        dataTypeList.add(schema.getType());
        valueChunkList.add(chunk);
        hasSelectedField = true;
      }
    } catch (final PipeRuntimeOutOfMemoryCriticalException | LoadRuntimeOutOfMemoryException e) {
      // The current field subset has not been published to a chunk reader yet. Restart that subset
      // from the same offset after memory pressure is relieved. Remove fields appended before the
      // failed reservation as well; otherwise the retry would expose duplicate columns in the
      // tablet schema.
      columnTypes.subList(deviceIdSize, columnTypes.size()).clear();
      measurementList.subList(deviceIdSize, measurementList.size()).clear();
      dataTypeList.subList(deviceIdSize, dataTypeList.size()).clear();
      offset = initialOffset;
      throw e;
    }

    if (offset >= fieldSchemaList.size()) {
      currentChunkMetadata = null;
    }

    if (!hasSelectedField) {
      this.chunkReader = null;
      this.batchData = null;
      return;
    }

    this.chunkReader = new TableChunkReader(timeChunk, valueChunkList, null);
    this.modsInfoList =
        ModsOperationUtil.initializeMeasurementMods(deviceID, measurementList, modifications);
  }

  private boolean areAllFieldsDeletedByMods(
      final IDeviceID currentDeviceID, final AbstractAlignedChunkMetadata alignedChunkMetadata) {
    if (modifications.isEmpty() || fieldSchemaList.isEmpty()) {
      return false;
    }

    for (final IMeasurementSchema schema : fieldSchemaList) {
      if (!ModsOperationUtil.isAllDeletedByMods(
          currentDeviceID,
          internMeasurementName(schema),
          alignedChunkMetadata.getStartTime(),
          alignedChunkMetadata.getEndTime(),
          modifications)) {
        return false;
      }
    }
    return true;
  }

  private boolean isFieldDeletedByMods(
      final String measurementID, final long startTime, final long endTime) {
    return !modifications.isEmpty()
        && ModsOperationUtil.isAllDeletedByMods(
            deviceID, measurementID, startTime, endTime, modifications);
  }

  private String internMeasurementName(final IMeasurementSchema schema) {
    if (schema instanceof MeasurementSchema) {
      tabletStringInternPool.intern((MeasurementSchema) schema);
    }
    return tabletStringInternPool.intern(schema.getMeasurementName());
  }

  private boolean fillMeasurementValueColumns(
      final BatchData data, final Tablet tablet, final int rowIndex) {
    final TsPrimitiveType[] primitiveTypes =
        Objects.nonNull(data.getVector()) ? data.getVector() : new TsPrimitiveType[0];
    boolean needFillTime = false;
    boolean hasNonDeletedField = dataTypeList.size() == deviceIdSize;

    for (int i = deviceIdSize, size = dataTypeList.size(); i < size; i++) {
      final TsPrimitiveType primitiveType =
          i - deviceIdSize < primitiveTypes.length ? primitiveTypes[i - deviceIdSize] : null;
      final boolean isDeleted = ModsOperationUtil.isDelete(data.currentTime(), modsInfoList.get(i));
      if (!isDeleted) {
        hasNonDeletedField = true;
      }
      if (primitiveType == null || isDeleted) {
        switch (dataTypeList.get(i)) {
          case TEXT:
          case BLOB:
          case STRING:
            PipeTabletUtils.putValue(tablet, rowIndex, i, dataTypeList.get(i), Binary.EMPTY_VALUE);
        }
        PipeTabletUtils.markNullValue(tablet, rowIndex, i);
        continue;
      }
      needFillTime = true;

      switch (dataTypeList.get(i)) {
        case BOOLEAN:
          PipeTabletUtils.putValue(
              tablet, rowIndex, i, dataTypeList.get(i), primitiveType.getBoolean());
          break;
        case INT32:
          PipeTabletUtils.putValue(
              tablet, rowIndex, i, dataTypeList.get(i), primitiveType.getInt());
          break;
        case DATE:
          PipeTabletUtils.putValue(
              tablet,
              rowIndex,
              i,
              dataTypeList.get(i),
              DateUtils.parseIntToLocalDate(primitiveType.getInt()));
          break;
        case INT64:
        case TIMESTAMP:
          PipeTabletUtils.putValue(
              tablet, rowIndex, i, dataTypeList.get(i), primitiveType.getLong());
          break;
        case FLOAT:
          PipeTabletUtils.putValue(
              tablet, rowIndex, i, dataTypeList.get(i), primitiveType.getFloat());
          break;
        case DOUBLE:
          PipeTabletUtils.putValue(
              tablet, rowIndex, i, dataTypeList.get(i), primitiveType.getDouble());
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary binary = primitiveType.getBinary();
          PipeTabletUtils.putValue(
              tablet,
              rowIndex,
              i,
              dataTypeList.get(i),
              Objects.isNull(binary) || Objects.isNull(binary.getValues())
                  ? Binary.EMPTY_VALUE
                  : binary);
          break;
        default:
          throw new UnSupportedDataTypeException(
              DataNodePipeMessages.UNSUPPORTED + primitiveType.getDataType());
      }
    }
    return needFillTime || hasNonDeletedField;
  }

  private void fillDeviceIdColumns(
      final IDeviceID deviceID, final Tablet tablet, final int rowIndex) {
    final String[] deviceIdSegments = (String[]) deviceID.getSegments();
    int i = 1;
    for (int totalColumns = deviceIdSegments.length; i < totalColumns; i++) {
      if (deviceIdSegments[i] == null) {
        PipeTabletUtils.putValue(
            tablet, rowIndex, i - 1, dataTypeList.get(i - 1), Binary.EMPTY_VALUE);
        PipeTabletUtils.markNullValue(tablet, rowIndex, i - 1);
        continue;
      }
      PipeTabletUtils.putValue(
          tablet, rowIndex, i - 1, dataTypeList.get(i - 1), deviceIdSegments[i]);
    }

    while (i <= deviceIdSize) {
      PipeTabletUtils.putValue(
          tablet, rowIndex, i - 1, dataTypeList.get(i - 1), Binary.EMPTY_VALUE);
      PipeTabletUtils.markNullValue(tablet, rowIndex, i - 1);
      i++;
    }
  }
}
