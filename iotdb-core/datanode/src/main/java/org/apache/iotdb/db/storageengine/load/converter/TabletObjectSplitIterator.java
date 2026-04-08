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

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.ObjectTypeUtils;

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
    this.originalTablet = tablet;
    this.rowSize = tablet.getRowSize();
    List<IMeasurementSchema> schemas = tablet.getSchemas();

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
          tablet.getTableName() != null ? tablet.getTableName() : tablet.getDeviceId();
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
      throw new IllegalArgumentException(
          "objectDir and tsFile cannot both be null when Tablet contains OBJECT columns.");
    }

    this.insertTargetName =
        tablet.getTableName() != null ? tablet.getTableName() : tablet.getDeviceId();

    // Extract common Tag columns
    if (tablet.getColumnTypes() != null) {
      for (int i = 0; i < schemas.size(); i++) {
        if (schemas.get(i) != null && tablet.getColumnTypes().get(i) == ColumnCategory.TAG) {
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

        Binary[] originalValues = (Binary[]) tablet.getValues()[i];
        extractedObjectValues.add(Arrays.copyOf(originalValues, tablet.getRowSize()));

        BitMap originalBm = tablet.getBitMaps() != null ? tablet.getBitMaps()[i] : null;
        extractedObjectBitMaps.add(
            originalBm != null
                ? new BitMap(
                    tablet.getMaxRowNumber(),
                    Arrays.copyOf(originalBm.getByteArray(), originalBm.getByteArray().length))
                : null);

        // Clear original array elegantly
        Arrays.fill(originalValues, Binary.EMPTY_VALUE);

        if (tablet.getBitMaps() == null) {
          tablet.initBitMaps();
        }
        if (tablet.getBitMaps()[i] == null) {
          tablet.getBitMaps()[i] = new BitMap(tablet.getMaxRowNumber());
        }
        tablet.getBitMaps()[i].markAll();

      } else {
        if (tablet.getColumnTypes() == null
            || tablet.getColumnTypes().get(i) == ColumnCategory.FIELD) {
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
                      "Invalid object content length, expected %d bytes but got %d bytes, "
                          + "relativePath=%s, offset=%d, declaredFileLength=%d.",
                      bytesToRead,
                      byteBuffer.remaining(),
                      relativePath,
                      currentFileOffset,
                      fileLength));
            }

            content = byteBuffer.array();
          } catch (Exception e) {
            LOGGER.warn("Failed to read object content via ObjectTypeUtils.", e);
            throw new RuntimeException("Failed to read object content via ObjectTypeUtils.", e);
          }
        } else {
          content = new byte[0];
        }

        boolean isEOF = currentFileOffset + totalBytesRead >= fileLength;

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
}
