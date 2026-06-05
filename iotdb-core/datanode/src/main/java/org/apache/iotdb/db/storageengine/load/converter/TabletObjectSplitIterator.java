/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.load.converter;

import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class TabletObjectSplitIterator implements Iterator<Tablet>, AutoCloseable {

  public static final Logger LOGGER = LoggerFactory.getLogger(TabletObjectSplitIterator.class);

  private final File searchRoot;

  private final Tablet originalTablet;
  private final int rowSize;
  private final List<Integer> objectColIndices = new ArrayList<>();
  private final List<Binary[]> extractedObjectValues = new ArrayList<>();
  private final List<BitMap> extractedObjectBitMaps = new ArrayList<>();

  private final String insertTargetName;

  // Tag schema and category templates for fast on-the-fly assembly
  private final List<IMeasurementSchema> tagColumnSchemas = new ArrayList<>();
  private final List<ColumnCategory> tagColumnCategories = new ArrayList<>();
  private final List<Integer> tagColumnIndexes = new ArrayList<>();

  // The index of the Object column in the newly generated chunkTablet is always fixed.
  // It is placed exactly after all Tag columns.
  private final int chunkObjColIdx;

  private final boolean hasNonObjectField;
  private boolean isBaseDataReturned = false;

  private int currentRow = 0;
  private int currentObjColIdx = 0;
  private long currentFileOffset = 0;

  private Tablet nextTabletCache = null;

  public TabletObjectSplitIterator(final Tablet tablet, final File tsFile, final File objectDir) {
    this(tablet, tsFile, objectDir, false);
  }

  public TabletObjectSplitIterator(
      final Tablet tablet, final File tsFile, final File objectDir, final boolean copyTablet) {
    this.originalTablet = copyTabletIfNecessary(tablet, copyTablet);
    this.rowSize = originalTablet.getRowSize();
    List<IMeasurementSchema> schemas = originalTablet.getSchemas();

    if (schemas == null || schemas.isEmpty()) {
      this.searchRoot = null;
      this.hasNonObjectField = true;
      this.insertTargetName = null;
      this.chunkObjColIdx = 0;
      return;
    }

    boolean containsObject = false;
    for (IMeasurementSchema schema : schemas) {
      if (schema != null && schema.getType() == TSDataType.OBJECT) {
        containsObject = true;
        break;
      }
    }

    if (!containsObject) {
      this.searchRoot = null;
      this.hasNonObjectField = true;
      this.insertTargetName =
          originalTablet.getTableName() != null
              ? originalTablet.getTableName()
              : originalTablet.getDeviceId();
      this.chunkObjColIdx = 0;
      return;
    }

    if (objectDir != null) {
      this.searchRoot = objectDir;
    } else if (tsFile != null) {
      String dirName =
          tsFile.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)
              ? tsFile
                  .getName()
                  .substring(0, tsFile.getName().length() - TsFileConstant.TSFILE_SUFFIX.length())
              : tsFile.getName();
      this.searchRoot = new File(tsFile.getParent(), dirName);
    } else {
      this.searchRoot = null;
    }

    this.insertTargetName =
        originalTablet.getTableName() != null
            ? originalTablet.getTableName()
            : originalTablet.getDeviceId();

    // Extract common Tag columns
    if (originalTablet.getColumnTypes() != null) {
      for (int i = 0; i < schemas.size(); i++) {
        if (schemas.get(i) != null
            && originalTablet.getColumnTypes().get(i) == ColumnCategory.TAG) {
          tagColumnSchemas.add(schemas.get(i));
          tagColumnCategories.add(ColumnCategory.TAG);
          tagColumnIndexes.add(i);
        }
      }
    }

    // The Object column will always be placed after all Tag columns
    this.chunkObjColIdx = tagColumnSchemas.size();

    boolean nonObjectFieldFound = false;

    // Process Object columns and identify non-object fields
    for (int i = 0; i < schemas.size(); i++) {
      IMeasurementSchema schema = schemas.get(i);
      if (schema == null) {
        continue;
      }

      if (schema.getType() == TSDataType.OBJECT) {
        objectColIndices.add(i);

        Binary[] originalValues = (Binary[]) originalTablet.getValues()[i];
        extractedObjectValues.add(Arrays.copyOf(originalValues, originalTablet.getRowSize()));

        BitMap originalBm =
            originalTablet.getBitMaps() != null ? originalTablet.getBitMaps()[i] : null;
        extractedObjectBitMaps.add(
            originalBm != null
                ? new BitMap(
                    originalTablet.getMaxRowNumber(),
                    Arrays.copyOf(originalBm.getByteArray(), originalBm.getByteArray().length))
                : null);

        // Clear original array elegantly
        Arrays.fill(originalValues, Binary.EMPTY_VALUE);

        if (originalTablet.getBitMaps() == null) {
          originalTablet.initBitMaps();
        }
        if (originalTablet.getBitMaps()[i] == null) {
          originalTablet.getBitMaps()[i] = new BitMap(originalTablet.getMaxRowNumber());
        }
        originalTablet.getBitMaps()[i].markAll();

      } else {
        if (originalTablet.getColumnTypes() == null
            || originalTablet.getColumnTypes().get(i) == ColumnCategory.FIELD) {
          nonObjectFieldFound = true;
        }
      }
    }

    this.hasNonObjectField = nonObjectFieldFound || objectColIndices.isEmpty();
  }

  @Override
  public boolean hasNext() {
    if (nextTabletCache == null) {
      nextTabletCache = tryComputeNext();
    }
    return nextTabletCache != null;
  }

  @Override
  public Tablet next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    Tablet result = nextTabletCache;
    nextTabletCache = null;
    return result;
  }

  private Tablet tryComputeNext() {
    if (!isBaseDataReturned) {
      isBaseDataReturned = true;
      if (hasNonObjectField) {
        return originalTablet;
      }
    }

    if (objectColIndices.isEmpty()) {
      return null;
    }

    while (currentObjColIdx < objectColIndices.size()) {
      while (currentRow < rowSize) {
        Binary val = extractedObjectValues.get(currentObjColIdx)[currentRow];
        BitMap originalBm = extractedObjectBitMaps.get(currentObjColIdx);
        boolean isOriginallyNull =
            (originalBm != null && originalBm.isMarked(currentRow)) || val == null;
        if (!isOriginallyNull) {
          break;
        }
        currentRow++;
        currentFileOffset = 0;
      }

      if (currentRow >= rowSize) {
        currentObjColIdx++;
        currentRow = 0;
        currentFileOffset = 0;
        continue;
      }

      // Get the original index of the current Object column
      int origObjIdx = objectColIndices.get(currentObjColIdx);

      // On-the-fly Assembly: Construct Schema List (Tags + Current Object)
      List<IMeasurementSchema> newSchemas = new ArrayList<>(tagColumnSchemas.size() + 1);
      newSchemas.addAll(tagColumnSchemas);
      newSchemas.add(originalTablet.getSchemas().get(origObjIdx));

      Tablet chunkTablet = new Tablet(insertTargetName, newSchemas, rowSize);

      // On-the-fly Assembly: Construct Category List
      if (originalTablet.getColumnTypes() != null) {
        List<ColumnCategory> newCategories = new ArrayList<>(tagColumnCategories.size() + 1);
        newCategories.addAll(tagColumnCategories);
        newCategories.add(originalTablet.getColumnTypes().get(origObjIdx));
        chunkTablet.setColumnCategories(newCategories);
      }

      chunkTablet.initBitMaps();
      chunkTablet.addTimestamp(0, 0);
      chunkTablet.setRowSize(0);
      for (BitMap bm : chunkTablet.getBitMaps()) {
        if (bm != null) {
          bm.markAll();
        }
      }

      int currentChunkSize = 0;
      int chunkRowCount = 0;

      final int chunkSizeLimitBytes =
          IoTDBDescriptor.getInstance()
              .getConfig()
              .getLoadTsFileObjectColumnChunkSizeLimitInBytes();

      while (currentRow < rowSize
          && currentChunkSize < chunkSizeLimitBytes
          && chunkRowCount < rowSize) {

        Binary val = extractedObjectValues.get(currentObjColIdx)[currentRow];
        BitMap originalBm = extractedObjectBitMaps.get(currentObjColIdx);
        boolean isOriginallyNull =
            (originalBm != null && originalBm.isMarked(currentRow)) || val == null;

        if (isOriginallyNull) {
          currentRow++;
          currentFileOffset = 0;
          continue;
        }

        if (searchRoot == null) {
          final ObjectChunk objectChunk = parseObjectValueContentBinary(val);
          appendTagColumns(chunkTablet, chunkRowCount);
          chunkTablet.getTimestamps()[chunkRowCount] = originalTablet.getTimestamps()[currentRow];
          chunkTablet.addValue(
              chunkRowCount,
              chunkObjColIdx,
              objectChunk.isEOF,
              objectChunk.offset,
              objectChunk.content);
          chunkTablet.getBitMaps()[chunkObjColIdx].unmark(chunkRowCount);

          currentChunkSize += objectChunk.content.length;
          chunkRowCount++;
          currentRow++;
          currentFileOffset = 0;
          continue;
        }

        Pair<Long, String> sizeAndPath = ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair(val);
        long fileLength = sizeAndPath.getLeft();
        String relativePath = sizeAndPath.getRight();

        int bytesToRead =
            (int) Math.min(chunkSizeLimitBytes - currentChunkSize, fileLength - currentFileOffset);
        if (bytesToRead <= 0) {
          bytesToRead = 0;
        }

        byte[] content;
        int totalBytesRead = 0;

        if (bytesToRead > 0) {
          try {
            ByteBuffer byteBuffer =
                ObjectTypeUtils.readObjectContent(
                    searchRoot, relativePath, currentFileOffset, bytesToRead);
            totalBytesRead = byteBuffer.remaining();
            if (byteBuffer.remaining() != bytesToRead) {
              throw new IllegalStateException(
                  String.format(
                      StorageEngineMessages.INVALID_OBJECT_CONTENT_LENGTH,
                      bytesToRead,
                      byteBuffer.remaining(),
                      relativePath,
                      currentFileOffset,
                      fileLength));
            }

            content = byteBuffer.array();
          } catch (Exception e) {
            LOGGER.warn(
                StorageEngineMessages.FAILED_TO_READ_OBJECT_CONTENT_VIA_OBJECT_TYPE_UTILS, e);
            throw new RuntimeException(
                StorageEngineMessages.FAILED_TO_READ_OBJECT_CONTENT_VIA_OBJECT_TYPE_UTILS, e);
          }
        } else {
          content = new byte[0];
        }

        boolean isEOF = currentFileOffset + totalBytesRead >= fileLength;

        appendTagColumns(chunkTablet, chunkRowCount);

        chunkTablet.getTimestamps()[chunkRowCount] = originalTablet.getTimestamps()[currentRow];
        chunkTablet.addValue(chunkRowCount, chunkObjColIdx, isEOF, currentFileOffset, content);
        chunkTablet.getBitMaps()[chunkObjColIdx].unmark(chunkRowCount);

        currentFileOffset += totalBytesRead;
        currentChunkSize += totalBytesRead;
        chunkRowCount++;

        if (isEOF) {
          currentRow++;
          currentFileOffset = 0;
        }
      }

      if (chunkRowCount > 0) {
        chunkTablet.setRowSize(chunkRowCount);
        return chunkTablet;
      }
    }

    return null;
  }

  @Override
  public void close() {
    nextTabletCache = null;
  }

  private static Tablet copyTabletIfNecessary(final Tablet tablet, final boolean copyTablet) {
    if (!copyTablet || tablet == null) {
      return tablet;
    }
    return cloneTablet(tablet);
  }

  private void appendTagColumns(final Tablet chunkTablet, final int chunkRowCount) {
    for (int t = 0; t < tagColumnIndexes.size(); t++) {
      int origTagIdx = tagColumnIndexes.get(t);
      boolean tagIsNull =
          originalTablet.getBitMaps() != null
              && originalTablet.getBitMaps()[origTagIdx] != null
              && originalTablet.getBitMaps()[origTagIdx].isMarked(currentRow);

      if (!tagIsNull) {
        chunkTablet.getBitMaps()[t].unmark(chunkRowCount);
        Object srcArray = originalTablet.getValues()[origTagIdx];
        Object destArray = chunkTablet.getValues()[t];
        ((Binary[]) destArray)[chunkRowCount] = ((Binary[]) srcArray)[currentRow];
      }
    }
  }

  private static ObjectChunk parseObjectValueContentBinary(final Binary binary) {
    final byte[] values = binary.getValues();
    if (values.length < 9 || (values[0] != 0 && values[0] != 1)) {
      throw new IllegalArgumentException(
          StorageEngineMessages.INVALID_OBJECT_VALUE_CONTENT_BINARY_EOF_AND_OFFSET);
    }

    final ByteBuffer buffer = ByteBuffer.wrap(values);
    final boolean isEOF = buffer.get() == 1;
    final long offset = buffer.getLong();
    if (offset < 0) {
      throw new IllegalArgumentException(
          String.format(
              StorageEngineMessages.INVALID_OBJECT_VALUE_CONTENT_BINARY_NEGATIVE_OFFSET, offset));
    }

    final byte[] content = new byte[buffer.remaining()];
    buffer.get(content);
    return new ObjectChunk(isEOF, offset, content);
  }

  private static boolean containsObjectColumn(final List<IMeasurementSchema> schemas) {
    if (schemas == null || schemas.isEmpty()) {
      return false;
    }
    for (IMeasurementSchema schema : schemas) {
      if (schema != null && schema.getType() == TSDataType.OBJECT) {
        return true;
      }
    }
    return false;
  }

  private static Tablet cloneTablet(final Tablet source) {
    final Tablet clonedTablet =
        new Tablet(source.getDeviceId(), source.getSchemas(), source.getRowSize());
    final int targetMaxRowNumber = clonedTablet.getMaxRowNumber();
    final int copyRowSize = Math.min(source.getRowSize(), targetMaxRowNumber);

    if (source.getColumnTypes() != null) {
      clonedTablet.setColumnCategories(new ArrayList<>(source.getColumnTypes()));
    }

    final long[] sourceTimestamps = source.getTimestamps();
    final long[] clonedTimestamps = clonedTablet.getTimestamps();
    if (sourceTimestamps != null && clonedTimestamps != null) {
      final int timestampCopyLength =
          Math.min(copyRowSize, Math.min(sourceTimestamps.length, clonedTimestamps.length));
      System.arraycopy(sourceTimestamps, 0, clonedTimestamps, 0, timestampCopyLength);
    }

    final Object[] sourceValues = source.getValues();
    final Object[] clonedValues = clonedTablet.getValues();
    final List<IMeasurementSchema> schemas = source.getSchemas();
    if (sourceValues != null && clonedValues != null) {
      for (int i = 0; i < sourceValues.length; i++) {
        final TSDataType type =
            schemas != null && i < schemas.size() && schemas.get(i) != null
                ? schemas.get(i).getType()
                : null;
        clonedValues[i] = cloneArray(sourceValues[i], type, copyRowSize);
      }
    }

    if (source.getBitMaps() != null) {
      clonedTablet.initBitMaps();
      for (int i = 0; i < source.getBitMaps().length; i++) {
        final BitMap bitMap = source.getBitMaps()[i];
        if (bitMap != null) {
          final int bitmapByteLength = (targetMaxRowNumber + 7) / 8;
          clonedTablet.getBitMaps()[i] =
              new BitMap(
                  targetMaxRowNumber, Arrays.copyOf(bitMap.getByteArray(), bitmapByteLength));
        } else {
          clonedTablet.getBitMaps()[i] = null;
        }
      }
    }

    clonedTablet.setRowSize(copyRowSize);
    return clonedTablet;
  }

  private static Object cloneArray(
      final Object sourceArray, final TSDataType type, final int copyLength) {
    if (sourceArray == null) {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        return Arrays.copyOf((boolean[]) sourceArray, copyLength);
      case INT32:
      case DATE:
        return Arrays.copyOf((int[]) sourceArray, copyLength);
      case INT64:
      case TIMESTAMP:
        return Arrays.copyOf((long[]) sourceArray, copyLength);
      case FLOAT:
        return Arrays.copyOf((float[]) sourceArray, copyLength);
      case DOUBLE:
        return Arrays.copyOf((double[]) sourceArray, copyLength);
      case TEXT:
      case STRING:
      case BLOB:
      case OBJECT:
        return Arrays.copyOf((Binary[]) sourceArray, copyLength);
      default:
        throw new IllegalArgumentException(
            String.format(
                StorageEngineMessages.UNSUPPORTED_TABLET_COLUMN_ARRAY_TYPE,
                sourceArray.getClass().getName(),
                type));
    }
  }

  private static final class ObjectChunk {
    private final boolean isEOF;
    private final long offset;
    private final byte[] content;

    private ObjectChunk(final boolean isEOF, final long offset, final byte[] content) {
      this.isEOF = isEOF;
      this.offset = offset;
      this.content = content;
    }
  }
}
