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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.payload.request;

import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.commons.exception.ObjectFileNotExist;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;

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
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class IoTConsensusV2ObjectFileUtils {

  private IoTConsensusV2ObjectFileUtils() {
    // Utility class
  }

  public static List<ObjectFileDescriptor> collectObjectFileDescriptors(
      final InsertNode insertNode) {
    final Map<String, ObjectFileDescriptor> objectFileDescriptors = new LinkedHashMap<>();
    collectObjectFileDescriptors(insertNode, objectFileDescriptors);
    return new ArrayList<>(objectFileDescriptors.values());
  }

  public static List<ObjectFileDescriptor> collectObjectFileDescriptors(
      final PipeTsFileInsertionEvent tsFileInsertionEvent) throws Exception {
    final Map<String, ObjectFileDescriptor> objectFileDescriptors = new LinkedHashMap<>();
    collectObjectFileDescriptors(tsFileInsertionEvent.getTsFile(), objectFileDescriptors);
    return new ArrayList<>(objectFileDescriptors.values());
  }

  private static void collectObjectFileDescriptors(
      final File tsFile, final Map<String, ObjectFileDescriptor> objectFileDescriptors)
      throws IOException {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final IMetadataQuerier metadataQuerier = new MetadataQuerierByFileImpl(reader);
      final TsFileMetadata fileMetadata = metadataQuerier.getWholeFileMetadata();
      final Map<String, TableSchema> tableSchemaMap = reader.getTableSchemaMap();
      if (tableSchemaMap == null) {
        return;
      }

      for (final Map.Entry<String, TableSchema> entry : tableSchemaMap.entrySet()) {
        if (!mayContainObjectValue(entry.getValue())) {
          continue;
        }

        final MetadataIndexNode tableRoot = fileMetadata.getTableMetadataIndexNode(entry.getKey());
        if (tableRoot == null) {
          continue;
        }

        final Iterator<Pair<IDeviceID, MetadataIndexNode>> deviceMetaIterator =
            metadataQuerier.deviceIterator(tableRoot, null);
        while (deviceMetaIterator.hasNext()) {
          final Pair<IDeviceID, MetadataIndexNode> pair = deviceMetaIterator.next();
          for (final AbstractAlignedChunkMetadata alignedChunkMetadata :
              reader.getAlignedChunkMetadata(pair.getLeft(), false)) {
            collectObjectFileDescriptors(reader, alignedChunkMetadata, objectFileDescriptors);
          }
        }
      }
    }
  }

  private static boolean mayContainObjectValue(final TableSchema tableSchema) {
    if (tableSchema == null || tableSchema.getColumnSchemas() == null) {
      return false;
    }
    for (final IMeasurementSchema schema : tableSchema.getColumnSchemas()) {
      if (schema != null && schema.getType() == TSDataType.OBJECT) {
        return true;
      }
    }
    return false;
  }

  private static void collectObjectFileDescriptors(
      final TsFileSequenceReader reader,
      final AbstractAlignedChunkMetadata alignedChunkMetadata,
      final Map<String, ObjectFileDescriptor> objectFileDescriptors)
      throws IOException {
    if (alignedChunkMetadata == null || alignedChunkMetadata.getValueChunkMetadataList() == null) {
      return;
    }

    final List<Chunk> objectChunks = new ArrayList<>();
    for (final IChunkMetadata chunkMetadata : alignedChunkMetadata.getValueChunkMetadataList()) {
      if (chunkMetadata == null || chunkMetadata.getDataType() != TSDataType.OBJECT) {
        continue;
      }
      objectChunks.add(reader.readMemChunk((ChunkMetadata) chunkMetadata));
    }

    if (objectChunks.isEmpty()) {
      return;
    }

    final Chunk timeChunk =
        reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
    timeChunk.getData().rewind();
    for (final Chunk objectChunk : objectChunks) {
      objectChunk.getData().rewind();
    }

    final IChunkReader chunkReader = new TableChunkReader(timeChunk, objectChunks, null);
    while (chunkReader.hasNextSatisfiedPage()) {
      final BatchData batchData = chunkReader.nextPageData();
      while (batchData.hasCurrent()) {
        final TsPrimitiveType[] objectValues = batchData.getVector();
        if (objectValues != null) {
          for (final TsPrimitiveType objectValue : objectValues) {
            if (objectValue != null) {
              collectObjectFileDescriptor(objectValue.getBinary(), objectFileDescriptors);
            }
          }
        }
        batchData.next();
      }
    }
  }

  private static void collectObjectFileDescriptors(
      final InsertNode insertNode, final Map<String, ObjectFileDescriptor> objectFileDescriptors) {
    if (insertNode instanceof InsertRowNode) {
      collectObjectFileDescriptors((InsertRowNode) insertNode, objectFileDescriptors);
      return;
    }
    if (insertNode instanceof InsertTabletNode) {
      collectObjectFileDescriptors((InsertTabletNode) insertNode, objectFileDescriptors);
      return;
    }
    if (insertNode instanceof InsertRowsNode) {
      for (final InsertRowNode insertRowNode :
          ((InsertRowsNode) insertNode).getInsertRowNodeList()) {
        collectObjectFileDescriptors(insertRowNode, objectFileDescriptors);
      }
      return;
    }
    if (insertNode instanceof InsertRowsOfOneDeviceNode) {
      for (final InsertRowNode insertRowNode :
          ((InsertRowsOfOneDeviceNode) insertNode).getInsertRowNodeList()) {
        collectObjectFileDescriptors(insertRowNode, objectFileDescriptors);
      }
      return;
    }
    if (insertNode instanceof InsertMultiTabletsNode) {
      for (final InsertTabletNode insertTabletNode :
          ((InsertMultiTabletsNode) insertNode).getInsertTabletNodeList()) {
        collectObjectFileDescriptors(insertTabletNode, objectFileDescriptors);
      }
    }
  }

  private static void collectObjectFileDescriptors(
      final InsertRowNode insertRowNode,
      final Map<String, ObjectFileDescriptor> objectFileDescriptors) {
    final TSDataType[] dataTypes = insertRowNode.getDataTypes();
    final Object[] values = insertRowNode.getValues();
    final String[] measurements = insertRowNode.getMeasurements();
    if (dataTypes == null || values == null) {
      return;
    }

    final int columnCount = Math.min(dataTypes.length, values.length);
    for (int i = 0; i < columnCount; ++i) {
      if ((measurements != null && measurements[i] == null)
          || dataTypes[i] != TSDataType.OBJECT
          || values[i] == null) {
        continue;
      }
      collectObjectFileDescriptor(values[i], objectFileDescriptors);
    }
  }

  private static void collectObjectFileDescriptors(
      final InsertTabletNode insertTabletNode,
      final Map<String, ObjectFileDescriptor> objectFileDescriptors) {
    final TSDataType[] dataTypes = insertTabletNode.getDataTypes();
    final Object[] columns = insertTabletNode.getColumns();
    final String[] measurements = insertTabletNode.getMeasurements();
    if (dataTypes == null || columns == null) {
      return;
    }

    final BitMap[] bitMaps = insertTabletNode.getBitMaps();
    final int columnCount = Math.min(dataTypes.length, columns.length);
    for (int i = 0; i < columnCount; ++i) {
      if ((measurements != null && measurements[i] == null)
          || dataTypes[i] != TSDataType.OBJECT
          || columns[i] == null) {
        continue;
      }

      final Binary[] objectValues = (Binary[]) columns[i];
      final BitMap bitMap = bitMaps == null || i >= bitMaps.length ? null : bitMaps[i];
      final int rowCount = Math.min(insertTabletNode.getRowCount(), objectValues.length);
      for (int row = 0; row < rowCount; ++row) {
        if (bitMap != null && bitMap.isMarked(row)) {
          continue;
        }
        collectObjectFileDescriptor(objectValues[row], objectFileDescriptors);
      }
    }
  }

  private static void collectObjectFileDescriptors(
      final Tablet tablet, final Map<String, ObjectFileDescriptor> objectFileDescriptors) {
    final List<IMeasurementSchema> schemas = tablet.getSchemas();
    final Object[] columns = tablet.getValues();
    if (schemas == null || columns == null) {
      return;
    }

    final BitMap[] bitMaps = tablet.getBitMaps();
    final int columnCount = Math.min(schemas.size(), columns.length);
    for (int i = 0; i < columnCount; ++i) {
      final IMeasurementSchema schema = schemas.get(i);
      if (schema == null || schema.getType() != TSDataType.OBJECT || columns[i] == null) {
        continue;
      }

      final BitMap bitMap = bitMaps == null || i >= bitMaps.length ? null : bitMaps[i];
      final int rowCount = tablet.getRowSize();
      for (int row = 0; row < rowCount; ++row) {
        if (bitMap != null && bitMap.isMarked(row)) {
          continue;
        }
        collectObjectFileDescriptor(getTabletObjectValue(columns[i], row), objectFileDescriptors);
      }
    }
  }

  private static Object getTabletObjectValue(final Object column, final int row) {
    if (column instanceof Binary[]) {
      return ((Binary[]) column)[row];
    }
    if (column instanceof byte[][]) {
      return new Binary(((byte[][]) column)[row]);
    }
    if (column instanceof Object[]) {
      return ((Object[]) column)[row];
    }
    throw new IllegalArgumentException(
        String.format(
            "Object type column should be Binary[], byte[][], or Object[], but actual type is %s.",
            column.getClass().getName()));
  }

  private static void collectObjectFileDescriptor(
      final Object value, final Map<String, ObjectFileDescriptor> objectFileDescriptors) {
    if (value == null) {
      return;
    }
    if (!(value instanceof Binary)) {
      throw new IllegalArgumentException(
          String.format(
              "Object type value should be Binary, but actual type is %s.",
              value.getClass().getName()));
    }

    final Binary binary = (Binary) value;
    if (binary.getValues() == null || binary.getLength() == 0) {
      return;
    }

    final Pair<Long, IObjectPath> objectSizePathPair =
        ObjectTypeUtils.parseObjectBinaryToSizeIObjectPathPair(binary);
    final long objectSize = objectSizePathPair.getLeft();
    if (objectSize < 0) {
      throw new IllegalArgumentException(
          String.format(
              "Object file size should be non-negative, but actual size is %s.", objectSize));
    }

    final IObjectPath objectPath = objectSizePathPair.getRight();
    final String objectPathString = objectPath.toString();
    final ObjectFileDescriptor existedDescriptor = objectFileDescriptors.get(objectPathString);
    if (existedDescriptor != null) {
      if (existedDescriptor.getObjectSize() != objectSize) {
        throw new IllegalArgumentException(
            String.format(
                "Object file %s has inconsistent sizes %s and %s.",
                objectPathString, existedDescriptor.getObjectSize(), objectSize));
      }
      return;
    }

    if (!TierManager.getInstance()
        .getAbsoluteObjectFilePath(objectPathString, true)
        .isPresent()) {
      throw new ObjectFileNotExist(objectPathString);
    }

    objectFileDescriptors.put(
        objectPathString, new ObjectFileDescriptor(objectSize, objectPath));
  }

  public static final class ObjectFileDescriptor {
    private final long objectSize;
    private final IObjectPath objectPath;

    private ObjectFileDescriptor(final long objectSize, final IObjectPath objectPath) {
      this.objectSize = objectSize;
      this.objectPath = objectPath;
    }

    public long getObjectSize() {
      return objectSize;
    }

    public IObjectPath getObjectPath() {
      return objectPath;
    }

    public String getObjectPathString() {
      return objectPath.toString();
    }
  }
}
