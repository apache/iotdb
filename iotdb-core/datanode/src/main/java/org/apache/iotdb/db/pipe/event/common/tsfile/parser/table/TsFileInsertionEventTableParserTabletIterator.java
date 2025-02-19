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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public class TsFileInsertionEventTableParserTabletIterator
    implements Iterator<Pair<Tablet, Integer>> {

  private final long startTime;
  private final long endTime;

  private final TsFileSequenceReader reader;
  private final IMetadataQuerier metadataQuerier;
  private final TsFileMetadata fileMetadata;
  private final Iterator<Map.Entry<String, TableSchema>> filteredTableSchemaIterator;

  private final PipeMemoryBlock allocatedMemoryBlockForTablet;
  private final PipeMemoryBlock allocatedMemoryBlockForBatchData;
  private final PipeMemoryBlock allocatedMemoryBlockForChunk;
  private final PipeMemoryBlock allocatedMemoryBlockForChunkMeta;

  private final AtomicInteger tabletUseMemorySize = new AtomicInteger(0);

  private IChunkReader chunkReader;
  private BatchData batchData;

  private Set<String> measurementNames;
  private Iterator<Pair<IDeviceID, MetadataIndexNode>> deviceMetaIterator;
  private Iterator<List<IChunkMetadata>> chunkMetadataList;

  private String tableName;
  private IDeviceID deviceID;
  private List<Tablet.ColumnCategory> columnTypes;
  private List<String> measurementList;
  private List<TSDataType> dataTypeList;

  private List<Integer> measurementColumIndexPairs = new ArrayList<>();
  private List<Integer> measurementIdIndexPairs = new ArrayList<>();

  private boolean isTheSameTableName;

  public TsFileInsertionEventTableParserTabletIterator(
      final TsFileSequenceReader tsFileSequenceReader,
      final Predicate<Map.Entry<String, TableSchema>> predicate,
      final long startTime,
      final long endTime)
      throws IOException {

    // Allocate empty memory block, will be resized later.
    this.allocatedMemoryBlockForChunk =
        PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);
    this.allocatedMemoryBlockForBatchData =
        PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);
    this.allocatedMemoryBlockForTablet =
        PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);
    this.allocatedMemoryBlockForChunkMeta =
        PipeDataNodeResourceManager.memory().forceAllocateForTabletWithRetry(0);

    this.startTime = startTime;
    this.endTime = endTime;

    this.reader = tsFileSequenceReader;
    this.metadataQuerier = new MetadataQuerierByFileImpl(reader);
    fileMetadata = this.metadataQuerier.getWholeFileMetadata();
    filteredTableSchemaIterator =
        fileMetadata.getTableSchemaMap().entrySet().stream().filter(predicate).iterator();
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
            if (chunkMetadataList != null && chunkMetadataList.hasNext()) {
              initChunkReader((AbstractAlignedChunkMetadata) chunkMetadataList.next().get(0));
              state = State.INIT_DATA;
              break;
            }
          case INIT_CHUNK_METADATA:
            if (deviceMetaIterator != null && deviceMetaIterator.hasNext()) {
              Pair<IDeviceID, MetadataIndexNode> pair = deviceMetaIterator.next();
              deviceID = pair.getLeft();
              long size = 0;
              List<List<IChunkMetadata>> chunkMetadatas =
                  reader.getIChunkMetadataList(pair.left, measurementNames, pair.right);
              for (List<IChunkMetadata> chunkMetadataList : chunkMetadatas) {
                if (chunkMetadataList == null
                    || chunkMetadataList.isEmpty()
                    || !(chunkMetadataList.get(0) instanceof AbstractAlignedChunkMetadata)) {
                  throw new PipeException("Unsupported chunk metadata type");
                }
                AbstractAlignedChunkMetadata alignedChunkMetadata =
                    (AbstractAlignedChunkMetadata) chunkMetadataList.get(0);
                size =
                    +PipeMemoryWeightUtil.calculateAlignedChunkMetaRamBytesUsed(
                        alignedChunkMetadata);
              }
              PipeDataNodeResourceManager.memory()
                  .forceResize(allocatedMemoryBlockForChunkMeta, size);
              chunkMetadataList = chunkMetadatas.iterator();

              state = State.INIT_CHUNK_READER;
              break;
            }
          case INIT_DEVICE_META:
            if (filteredTableSchemaIterator != null && filteredTableSchemaIterator.hasNext()) {
              Map.Entry<String, TableSchema> entry = filteredTableSchemaIterator.next();
              tableName = entry.getKey();
              MetadataIndexNode tableRoot = fileMetadata.getTableMetadataIndexNode(tableName);
              deviceMetaIterator = metadataQuerier.deviceIterator(tableRoot, null);
              final TableSchema tableSchema = entry.getValue();

              final int columnSchemaSize = tableSchema.getColumnSchemas().size();
              dataTypeList = new ArrayList<>(columnSchemaSize);
              columnTypes = new ArrayList<>(columnSchemaSize);
              measurementList = new ArrayList<>(columnSchemaSize);
              measurementNames = new HashSet<>();

              measurementColumIndexPairs = new ArrayList<>(columnSchemaSize);
              measurementIdIndexPairs = new ArrayList<>(columnSchemaSize);

              for (int i = 0, size = tableSchema.getColumnSchemas().size(); i < size; i++) {
                final IMeasurementSchema schema = tableSchema.getColumnSchemas().get(i);
                final Tablet.ColumnCategory columnCategory = tableSchema.getColumnTypes().get(i);
                if (schema.getMeasurementName() != null && !schema.getMeasurementName().isEmpty()) {
                  columnTypes.add(columnCategory);
                  measurementList.add(schema.getMeasurementName());
                  dataTypeList.add(schema.getType());
                  if (!Tablet.ColumnCategory.TAG.equals(columnCategory)) {
                    measurementNames.add(schema.getMeasurementName());
                    measurementColumIndexPairs.add(i);
                  } else {
                    measurementIdIndexPairs.add(i);
                  }
                }
              }
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
  public Pair<Tablet, Integer> next() {
    return buildNextTablet();
  }

  private Pair<Tablet, Integer> buildNextTablet() {
    Tablet tablet = null;
    int size = 0;

    boolean isFirstRow = true;
    while (hasNext() && (isFirstRow || isTheSameTableName)) {
      if (batchData.currentTime() >= startTime && batchData.currentTime() <= endTime) {
        if (isFirstRow) {
          isTheSameTableName = true;
          // Calculate row count and memory size of the tablet based on the first row
          Pair<Integer, Integer> rowCountAndMemorySize =
              PipeMemoryWeightUtil.calculateTabletRowCountAndMemory(batchData);
          tabletUseMemorySize.addAndGet(size = rowCountAndMemorySize.getLeft());

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
      tablet = new Tablet(tableName, measurementList, dataTypeList, columnTypes, 0);
    }

    return new Pair<>(tablet, size);
  }

  private void initChunkReader(AbstractAlignedChunkMetadata alignedChunkMetadata)
      throws IOException {
    Chunk timeChunk =
        reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
    List<Chunk> valueChunkList = new ArrayList<>();

    for (IChunkMetadata metadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      if (metadata != null) {
        valueChunkList.add(reader.readMemChunk((ChunkMetadata) metadata));
      } else {
        valueChunkList.add((Chunk) null);
      }
    }

    PipeDataNodeResourceManager.memory()
        .forceResize(
            allocatedMemoryBlockForChunk,
            PipeMemoryWeightUtil.calculateChunkRamBytesUsed(timeChunk, valueChunkList));

    this.chunkReader = new TableChunkReader(timeChunk, valueChunkList, null);
  }

  private void fillMeasurementValueColumns(
      final BatchData data, final Tablet tablet, final int rowIndex) {
    final TsPrimitiveType[] primitiveTypes = data.getVector();
    final List<IMeasurementSchema> measurementSchemas = tablet.getSchemas();
    for (int i = 0, size = primitiveTypes.length; i < size; ++i) {
      final TsPrimitiveType primitiveType = primitiveTypes[i];
      if (Objects.isNull(primitiveType)) {
        continue;
      }
      switch (measurementSchemas.get(measurementColumIndexPairs.get(i)).getType()) {
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
    String[] deviceIdSegments = (String[]) deviceID.getSegments();
    int deviceIdSegmentSize = deviceIdSegments.length;

    for (int i = 0, totalColumns = measurementIdIndexPairs.size(); i < totalColumns; i++) {
      String valueToAdd = (i < deviceIdSegmentSize) ? deviceIdSegments[i] : null;
      tablet.addValue(rowIndex, measurementIdIndexPairs.get(i), valueToAdd);
    }
  }
}
