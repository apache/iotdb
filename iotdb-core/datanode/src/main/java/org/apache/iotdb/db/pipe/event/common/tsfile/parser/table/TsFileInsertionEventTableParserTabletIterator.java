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

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryWeightUtil;
import org.apache.iotdb.pipe.api.exception.PipeException;

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
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TsFileInsertionEventTableParserTabletIterator implements Iterator<Tablet> {
  private final int PIPE_MAX_ALIGNED_SERIES_NUM_IN_ONE_BATCH =
      PipeConfig.getInstance().getPipeMaxAlignedSeriesNumInOneBatch();
  private final long startTime;
  private final long endTime;

  // Used to read or record TSFileMeta tools or meta information
  private final TsFileSequenceReader reader;
  private final IMetadataQuerier metadataQuerier;
  private final TsFileMetadata fileMetadata;
  private final Iterator<Map.Entry<String, TableSchema>> filteredTableSchemaIterator;

  // For memory control
  private final PipeMemoryBlock allocatedMemoryBlockForTablet;
  private final PipeMemoryBlock allocatedMemoryBlockForBatchData;
  private final PipeMemoryBlock allocatedMemoryBlockForChunk;
  private final PipeMemoryBlock allocatedMemoryBlockForChunkMeta;
  private final PipeMemoryBlock allocatedMemoryBlockForTableSchema;

  // Used to read tsfile data
  private IChunkReader chunkReader;
  private BatchData batchData;

  // Record the metadata information of the currently read Table
  private Iterator<Pair<IDeviceID, MetadataIndexNode>> deviceMetaIterator;
  private Iterator<AbstractAlignedChunkMetadata> chunkMetadataList;
  private Iterator<IChunkMetadata> chunkMetadata;
  private AbstractAlignedChunkMetadata currentChunkMetadata;
  private Chunk timeChunk;
  private long timeChunkSize;
  private int offset;

  // Record the information of the currently read Table
  private String tableName;
  private IDeviceID deviceID;
  private List<Tablet.ColumnCategory> columnTypes;
  private List<String> measurementList;
  private List<TSDataType> dataTypeList;
  private int deviceIdSize;

  // Used to record whether the same Tablet is generated when parsing starts. Different table
  // information cannot be placed in the same Tablet.
  private boolean isSameTableName;
  private boolean isSameDeviceID;

  public TsFileInsertionEventTableParserTabletIterator(
      final TsFileSequenceReader tsFileSequenceReader,
      final Predicate<Map.Entry<String, TableSchema>> predicate,
      final PipeMemoryBlock allocatedMemoryBlockForTablet,
      final PipeMemoryBlock allocatedMemoryBlockForBatchData,
      final PipeMemoryBlock allocatedMemoryBlockForChunk,
      final PipeMemoryBlock allocatedMemoryBlockForChunkMeta,
      final PipeMemoryBlock allocatedMemoryBlockForTableSchema,
      final long startTime,
      final long endTime)
      throws IOException {

    this.startTime = startTime;
    this.endTime = endTime;

    this.reader = tsFileSequenceReader;
    this.metadataQuerier = new MetadataQuerierByFileImpl(reader);
    fileMetadata = this.metadataQuerier.getWholeFileMetadata();
    final List<Map.Entry<String, TableSchema>> tableSchemaList =
        fileMetadata.getTableSchemaMap().entrySet().stream()
            .filter(predicate)
            .collect(Collectors.toList());

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
      PipeDataNodeResourceManager.memory()
          .forceResize(this.allocatedMemoryBlockForTableSchema, tableSchemaSize);
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
              PipeDataNodeResourceManager.memory()
                  .forceResize(
                      allocatedMemoryBlockForBatchData,
                      PipeMemoryWeightUtil.calculateBatchDataRamBytesUsed(batchData));
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
            if (deviceMetaIterator != null && deviceMetaIterator.hasNext()) {
              final Pair<IDeviceID, MetadataIndexNode> pair = deviceMetaIterator.next();

              long size = 0;
              List<AbstractAlignedChunkMetadata> iChunkMetadataList =
                  reader.getAlignedChunkMetadata(pair.left, true);

              Iterator<AbstractAlignedChunkMetadata> chunkMetadataIterator =
                  iChunkMetadataList.iterator();
              while (chunkMetadataIterator.hasNext()) {
                final AbstractAlignedChunkMetadata alignedChunkMetadata =
                    chunkMetadataIterator.next();
                if (alignedChunkMetadata == null) {
                  throw new PipeException(
                      "Table model tsfile parsing does not support this type of ChunkMeta");
                }

                // Reduce the number of times Chunks are read
                if (alignedChunkMetadata.getEndTime() < startTime
                    || alignedChunkMetadata.getStartTime() > endTime) {
                  chunkMetadataIterator.remove();
                  continue;
                }

                size +=
                    PipeMemoryWeightUtil.calculateAlignedChunkMetaBytesUsed(alignedChunkMetadata);
                PipeDataNodeResourceManager.memory()
                    .forceResize(allocatedMemoryBlockForChunkMeta, size);
              }

              deviceID = pair.getLeft();
              chunkMetadataList = iChunkMetadataList.iterator();

              state = State.INIT_CHUNK_READER;
              break;
            }
          case INIT_DEVICE_META:
            if (filteredTableSchemaIterator != null && filteredTableSchemaIterator.hasNext()) {
              final Map.Entry<String, TableSchema> entry = filteredTableSchemaIterator.next();
              tableName = entry.getKey();
              final TableSchema tableSchema = entry.getValue();
              // The table name has changed, set to false
              isSameTableName = false;

              final MetadataIndexNode tableRoot = fileMetadata.getTableMetadataIndexNode(tableName);
              deviceMetaIterator = metadataQuerier.deviceIterator(tableRoot, null);

              final int columnSchemaSize = tableSchema.getColumnSchemas().size();
              dataTypeList = new ArrayList<>(PIPE_MAX_ALIGNED_SERIES_NUM_IN_ONE_BATCH);
              columnTypes = new ArrayList<>(PIPE_MAX_ALIGNED_SERIES_NUM_IN_ONE_BATCH);
              measurementList = new ArrayList<>(PIPE_MAX_ALIGNED_SERIES_NUM_IN_ONE_BATCH);

              for (int i = 0; i < columnSchemaSize; i++) {
                final IMeasurementSchema schema = tableSchema.getColumnSchemas().get(i);
                final Tablet.ColumnCategory columnCategory = tableSchema.getColumnTypes().get(i);
                if (schema != null
                    && schema.getMeasurementName() != null
                    && !schema.getMeasurementName().isEmpty()) {
                  final String measurementName = schema.getMeasurementName();
                  if (Tablet.ColumnCategory.TAG.equals(columnCategory)) {
                    columnTypes.add(Tablet.ColumnCategory.TAG);
                    measurementList.add(measurementName);
                    dataTypeList.add(schema.getType());
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
    Tablet tablet = null;

    boolean isFirstRow = true;
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
          PipeDataNodeResourceManager.memory()
              .forceResize(allocatedMemoryBlockForTablet, rowCountAndMemorySize.getLeft());

          tablet =
              new Tablet(
                  tableName,
                  measurementList,
                  dataTypeList,
                  columnTypes,
                  rowCountAndMemorySize.getLeft());
          tablet.initBitMaps();
          isFirstRow = false;
        }
        final int rowIndex = tablet.getRowSize();
        if (rowIndex >= tablet.getMaxRowNumber()) {
          break;
        }

        tablet.addTimestamp(rowIndex, batchData.currentTime());
        fillMeasurementValueColumns(batchData, tablet, rowIndex);
        fillDeviceIdColumns(deviceID, tablet, rowIndex);
      }

      if (batchData != null) {
        batchData.next();
      }
    }

    if (isFirstRow) {
      PipeDataNodeResourceManager.memory().forceResize(allocatedMemoryBlockForTablet, 0);
      tablet = new Tablet(tableName, measurementList, dataTypeList, columnTypes, 0);
      tablet.initBitMaps();
    }

    return tablet;
  }

  private void initChunkReader(final AbstractAlignedChunkMetadata alignedChunkMetadata)
      throws IOException {
    if (Objects.isNull(timeChunk)) {
      timeChunk = reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
      timeChunkSize = PipeMemoryWeightUtil.calculateChunkRamBytesUsed(timeChunk);
      PipeDataNodeResourceManager.memory().forceResize(allocatedMemoryBlockForChunk, timeChunkSize);
    }
    timeChunk.getData().rewind();
    long size = timeChunkSize;

    final List<Chunk> valueChunkList = new ArrayList<>(PIPE_MAX_ALIGNED_SERIES_NUM_IN_ONE_BATCH);

    // To ensure that the Tablet has the same alignedChunk column as the current one,
    // you need to create a new Tablet to fill in the data.
    isSameDeviceID = false;

    // Need to ensure that columnTypes recreates an array
    final List<Tablet.ColumnCategory> categories =
        new ArrayList<>(deviceIdSize + PIPE_MAX_ALIGNED_SERIES_NUM_IN_ONE_BATCH);
    for (int i = 0; i < deviceIdSize; i++) {
      categories.add(Tablet.ColumnCategory.TAG);
    }
    columnTypes = categories;

    // Clean up the remaining non-DeviceID column information
    measurementList.subList(deviceIdSize, measurementList.size()).clear();
    dataTypeList.subList(deviceIdSize, dataTypeList.size()).clear();

    final int startOffset = offset;
    for (; offset < alignedChunkMetadata.getValueChunkMetadataList().size(); ++offset) {
      final IChunkMetadata metadata = alignedChunkMetadata.getValueChunkMetadataList().get(offset);
      if (metadata != null) {
        // Record the column information corresponding to Meta to fill in Tablet
        columnTypes.add(Tablet.ColumnCategory.FIELD);
        measurementList.add(metadata.getMeasurementUid());
        dataTypeList.add(metadata.getDataType());

        final Chunk chunk = reader.readMemChunk((ChunkMetadata) metadata);
        size += PipeMemoryWeightUtil.calculateChunkRamBytesUsed(chunk);
        PipeDataNodeResourceManager.memory().forceResize(allocatedMemoryBlockForChunk, size);

        valueChunkList.add(chunk);
      }
      if (offset - startOffset >= PIPE_MAX_ALIGNED_SERIES_NUM_IN_ONE_BATCH) {
        break;
      }
    }

    if (offset >= alignedChunkMetadata.getValueChunkMetadataList().size()) {
      currentChunkMetadata = null;
    }

    this.chunkReader = new TableChunkReader(timeChunk, valueChunkList, null);
  }

  private void fillMeasurementValueColumns(
      final BatchData data, final Tablet tablet, final int rowIndex) {
    final TsPrimitiveType[] primitiveTypes = data.getVector();

    for (int i = deviceIdSize, size = dataTypeList.size(); i < size; i++) {
      final TsPrimitiveType primitiveType = primitiveTypes[i - deviceIdSize];
      if (primitiveType == null) {
        continue;
      }

      switch (dataTypeList.get(i)) {
        case BOOLEAN:
          tablet.addValue(rowIndex, i, primitiveType.getBoolean());
          break;
        case INT32:
          tablet.addValue(rowIndex, i, primitiveType.getInt());
          break;
        case DATE:
          tablet.addValue(rowIndex, i, DateUtils.parseIntToLocalDate(primitiveType.getInt()));
          break;
        case INT64:
        case TIMESTAMP:
          tablet.addValue(rowIndex, i, primitiveType.getLong());
          break;
        case FLOAT:
          tablet.addValue(rowIndex, i, primitiveType.getFloat());
          break;
        case DOUBLE:
          tablet.addValue(rowIndex, i, primitiveType.getDouble());
          break;
        case TEXT:
        case BLOB:
        case STRING:
          tablet.addValue(rowIndex, i, primitiveType.getBinary().getValues());
          break;
        default:
          throw new UnSupportedDataTypeException("UnSupported" + primitiveType.getDataType());
      }
    }
  }

  private void fillDeviceIdColumns(
      final IDeviceID deviceID, final Tablet tablet, final int rowIndex) {
    final String[] deviceIdSegments = (String[]) deviceID.getSegments();
    for (int i = 1, totalColumns = deviceIdSegments.length; i < totalColumns; i++) {
      if (deviceIdSegments[i] == null) {
        continue;
      }
      tablet.addValue(rowIndex, i - 1, deviceIdSegments[i]);
    }
  }
}
