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

package org.apache.tsfile.read.reader.page;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.encoding.decoder.Decoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.BatchDataFactory;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class ValuePageReader {

  private static final int MASK = 0x80;

  private final PageHeader pageHeader;

  private final TSDataType dataType;

  /** decoder for value column */
  private final Decoder valueDecoder;

  private byte[] bitmap;

  private int size;

  /** value column in memory */
  protected ByteBuffer valueBuffer;

  /** A list of deleted intervals. */
  private List<TimeRange> deleteIntervalList;

  private int deleteCursor = 0;

  private LazyLoadPageData lazyLoadPageData;

  public ValuePageReader(
      PageHeader pageHeader, ByteBuffer pageData, TSDataType dataType, Decoder valueDecoder) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.pageHeader = pageHeader;
    if (pageData != null) {
      splitDataToBitmapAndValue(pageData);
    }
    this.valueBuffer = pageData;
  }

  public ValuePageReader(
      PageHeader pageHeader,
      LazyLoadPageData lazyLoadPageData,
      TSDataType dataType,
      Decoder valueDecoder) {
    this.dataType = dataType;
    this.valueDecoder = valueDecoder;
    this.pageHeader = pageHeader;
    this.lazyLoadPageData = lazyLoadPageData;
  }

  private void splitDataToBitmapAndValue(ByteBuffer pageData) {
    if (!pageData.hasRemaining()) { // Empty Page
      return;
    }
    this.size = ReadWriteIOUtils.readInt(pageData);
    this.bitmap = new byte[(size + 7) / 8];
    pageData.get(bitmap);
    this.valueBuffer = pageData.slice();
  }

  /** Call this method before accessing data. */
  private void uncompressDataIfNecessary() throws IOException {
    if (lazyLoadPageData != null && valueBuffer == null) {
      ByteBuffer pageData = lazyLoadPageData.uncompressPageData(pageHeader);
      splitDataToBitmapAndValue(pageData);
      this.valueBuffer = pageData;
      lazyLoadPageData = null;
    }
  }

  /**
   * return a BatchData with the corresponding timeBatch, the BatchData's dataType is same as this
   * sub sensor
   */
  public BatchData nextBatch(long[] timeBatch, boolean ascending, Filter filter)
      throws IOException {
    uncompressDataIfNecessary();
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
    for (int i = 0; i < timeBatch.length; i++) {
      if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
        continue;
      }
      long timestamp = timeBatch[i];
      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          if (!isDeleted(timestamp)
              && (filter == null || filter.satisfyBoolean(timestamp, aBoolean))) {
            pageData.putBoolean(timestamp, aBoolean);
          }
          break;
        case INT32:
        case DATE:
          int anInt = valueDecoder.readInt(valueBuffer);
          if (!isDeleted(timestamp)
              && (filter == null || filter.satisfyInteger(timestamp, anInt))) {
            pageData.putInt(timestamp, anInt);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long aLong = valueDecoder.readLong(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfyLong(timestamp, aLong))) {
            pageData.putLong(timestamp, aLong);
          }
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfyFloat(timestamp, aFloat))) {
            pageData.putFloat(timestamp, aFloat);
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (!isDeleted(timestamp)
              && (filter == null || filter.satisfyDouble(timestamp, aDouble))) {
            pageData.putDouble(timestamp, aDouble);
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (!isDeleted(timestamp)
              && (filter == null || filter.satisfyBinary(timestamp, aBinary))) {
            pageData.putBinary(timestamp, aBinary);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return pageData.flip();
  }

  public TsPrimitiveType nextValue(long timestamp, int timeIndex) throws IOException {
    uncompressDataIfNecessary();
    TsPrimitiveType resultValue = null;
    if (valueBuffer == null || ((bitmap[timeIndex / 8] & 0xFF) & (MASK >>> (timeIndex % 8))) == 0) {
      return null;
    }
    switch (dataType) {
      case BOOLEAN:
        boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
        if (!isDeleted(timestamp)) {
          resultValue = new TsPrimitiveType.TsBoolean(aBoolean);
        }
        break;
      case INT32:
      case DATE:
        int anInt = valueDecoder.readInt(valueBuffer);
        if (!isDeleted(timestamp)) {
          resultValue = new TsPrimitiveType.TsInt(anInt);
        }
        break;
      case INT64:
      case TIMESTAMP:
        long aLong = valueDecoder.readLong(valueBuffer);
        if (!isDeleted(timestamp)) {
          resultValue = new TsPrimitiveType.TsLong(aLong);
        }
        break;
      case FLOAT:
        float aFloat = valueDecoder.readFloat(valueBuffer);
        if (!isDeleted(timestamp)) {
          resultValue = new TsPrimitiveType.TsFloat(aFloat);
        }
        break;
      case DOUBLE:
        double aDouble = valueDecoder.readDouble(valueBuffer);
        if (!isDeleted(timestamp)) {
          resultValue = new TsPrimitiveType.TsDouble(aDouble);
        }
        break;
      case TEXT:
      case BLOB:
      case STRING:
        Binary aBinary = valueDecoder.readBinary(valueBuffer);
        if (!isDeleted(timestamp)) {
          resultValue = new TsPrimitiveType.TsBinary(aBinary);
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }

    return resultValue;
  }

  /**
   * return the value array of the corresponding time, if this sub sensor don't have a value in a
   * time, just fill it with null
   */
  public TsPrimitiveType[] nextValueBatch(long[] timeBatch) throws IOException {
    uncompressDataIfNecessary();
    TsPrimitiveType[] valueBatch = new TsPrimitiveType[size];
    if (valueBuffer == null) {
      return valueBatch;
    }
    for (int i = 0; i < size; i++) {
      if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
        continue;
      }
      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          if (!isDeleted(timeBatch[i])) {
            valueBatch[i] = new TsPrimitiveType.TsBoolean(aBoolean);
          }
          break;
        case INT32:
        case DATE:
          int anInt = valueDecoder.readInt(valueBuffer);
          if (!isDeleted(timeBatch[i])) {
            valueBatch[i] = new TsPrimitiveType.TsInt(anInt);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long aLong = valueDecoder.readLong(valueBuffer);
          if (!isDeleted(timeBatch[i])) {
            valueBatch[i] = new TsPrimitiveType.TsLong(aLong);
          }
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (!isDeleted(timeBatch[i])) {
            valueBatch[i] = new TsPrimitiveType.TsFloat(aFloat);
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (!isDeleted(timeBatch[i])) {
            valueBatch[i] = new TsPrimitiveType.TsDouble(aDouble);
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (!isDeleted(timeBatch[i])) {
            valueBatch[i] = new TsPrimitiveType.TsBinary(aBinary);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return valueBatch;
  }

  public void writeColumnBuilderWithNextBatch(
      int readEndIndex, ColumnBuilder columnBuilder, boolean[] keepCurrentRow, boolean[] isDeleted)
      throws IOException {
    uncompressDataIfNecessary();
    if (valueBuffer == null) {
      for (int i = 0; i < readEndIndex; i++) {
        if (keepCurrentRow[i]) {
          columnBuilder.appendNull();
        }
      }
      return;
    }
    for (int i = 0; i < readEndIndex; i++) {
      if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
        if (keepCurrentRow[i]) {
          columnBuilder.appendNull();
        }
        continue;
      }
      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          if (keepCurrentRow[i]) {
            if (isDeleted[i]) {
              columnBuilder.appendNull();
            } else {
              columnBuilder.writeBoolean(aBoolean);
            }
          }
          break;
        case INT32:
        case DATE:
          int anInt = valueDecoder.readInt(valueBuffer);
          if (keepCurrentRow[i]) {
            if (isDeleted[i]) {
              columnBuilder.appendNull();
            } else {
              columnBuilder.writeInt(anInt);
            }
          }
          break;
        case INT64:
        case TIMESTAMP:
          long aLong = valueDecoder.readLong(valueBuffer);
          if (keepCurrentRow[i]) {
            if (isDeleted[i]) {
              columnBuilder.appendNull();
            } else {
              columnBuilder.writeLong(aLong);
            }
          }
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (keepCurrentRow[i]) {
            if (isDeleted[i]) {
              columnBuilder.appendNull();
            } else {
              columnBuilder.writeFloat(aFloat);
            }
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (keepCurrentRow[i]) {
            if (isDeleted[i]) {
              columnBuilder.appendNull();
            } else {
              columnBuilder.writeDouble(aDouble);
            }
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (keepCurrentRow[i]) {
            if (isDeleted[i]) {
              columnBuilder.appendNull();
            } else {
              columnBuilder.writeBinary(aBinary);
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
  }

  public void writeColumnBuilderWithNextBatch(
      int readEndIndex, ColumnBuilder columnBuilder, boolean[] keepCurrentRow) throws IOException {
    uncompressDataIfNecessary();
    if (valueBuffer == null) {
      for (int i = 0; i < readEndIndex; i++) {
        if (keepCurrentRow[i]) {
          columnBuilder.appendNull();
        }
      }
      return;
    }
    for (int i = 0; i < readEndIndex; i++) {
      if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
        if (keepCurrentRow[i]) {
          columnBuilder.appendNull();
        }
        continue;
      }
      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          if (keepCurrentRow[i]) {
            columnBuilder.writeBoolean(aBoolean);
          }
          break;
        case INT32:
        case DATE:
          int anInt = valueDecoder.readInt(valueBuffer);
          if (keepCurrentRow[i]) {
            columnBuilder.writeInt(anInt);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long aLong = valueDecoder.readLong(valueBuffer);
          if (keepCurrentRow[i]) {
            columnBuilder.writeLong(aLong);
          }
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (keepCurrentRow[i]) {
            columnBuilder.writeFloat(aFloat);
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (keepCurrentRow[i]) {
            columnBuilder.writeDouble(aDouble);
          }
          break;
        case TEXT:
        case BLOB:
        case STRING:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (keepCurrentRow[i]) {
            columnBuilder.writeBinary(aBinary);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
  }

  public void writeColumnBuilderWithNextBatch(
      int readStartIndex, int readEndIndex, ColumnBuilder columnBuilder) throws IOException {
    uncompressDataIfNecessary();
    if (valueBuffer == null) {
      columnBuilder.appendNull(readEndIndex - readStartIndex);
      return;
    }

    switch (dataType) {
      case BOOLEAN:
        // skip useless data
        for (int i = 0; i < readStartIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            continue;
          }
          valueDecoder.readBoolean(valueBuffer);
        }

        for (int i = readStartIndex; i < readEndIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            columnBuilder.appendNull();
            continue;
          }
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          columnBuilder.writeBoolean(aBoolean);
        }
        break;
      case INT32:
      case DATE:
        // skip useless data
        for (int i = 0; i < readStartIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            continue;
          }
          valueDecoder.readInt(valueBuffer);
        }

        for (int i = readStartIndex; i < readEndIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            columnBuilder.appendNull();
            continue;
          }
          int aInt = valueDecoder.readInt(valueBuffer);
          columnBuilder.writeInt(aInt);
        }
        break;
      case INT64:
      case TIMESTAMP:
        // skip useless data
        for (int i = 0; i < readStartIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            continue;
          }
          valueDecoder.readLong(valueBuffer);
        }

        for (int i = readStartIndex; i < readEndIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            columnBuilder.appendNull();
            continue;
          }
          long aLong = valueDecoder.readLong(valueBuffer);
          columnBuilder.writeLong(aLong);
        }
        break;
      case FLOAT:
        // skip useless data
        for (int i = 0; i < readStartIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            continue;
          }
          valueDecoder.readFloat(valueBuffer);
        }

        for (int i = readStartIndex; i < readEndIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            columnBuilder.appendNull();
            continue;
          }
          float aFloat = valueDecoder.readFloat(valueBuffer);
          columnBuilder.writeFloat(aFloat);
        }
        break;
      case DOUBLE:
        // skip useless data
        for (int i = 0; i < readStartIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            continue;
          }
          valueDecoder.readDouble(valueBuffer);
        }

        for (int i = readStartIndex; i < readEndIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            columnBuilder.appendNull();
            continue;
          }
          double aDouble = valueDecoder.readDouble(valueBuffer);
          columnBuilder.writeDouble(aDouble);
        }
        break;
      case TEXT:
      case BLOB:
      case STRING:
        // skip useless data
        for (int i = 0; i < readStartIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            continue;
          }
          valueDecoder.readBinary(valueBuffer);
        }

        for (int i = readStartIndex; i < readEndIndex; i++) {
          if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
            columnBuilder.appendNull();
            continue;
          }
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          columnBuilder.writeBinary(aBinary);
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.valueOf(dataType));
    }
  }

  public Statistics<? extends Serializable> getStatistics() {
    return pageHeader.getStatistics();
  }

  public void setDeleteIntervalList(List<TimeRange> list) {
    this.deleteIntervalList = list;
  }

  public List<TimeRange> getDeleteIntervalList() {
    return deleteIntervalList;
  }

  public boolean isModified() {
    return pageHeader.isModified();
  }

  public boolean isDeleted(long timestamp) {
    while (deleteIntervalList != null && deleteCursor < deleteIntervalList.size()) {
      if (deleteIntervalList.get(deleteCursor).contains(timestamp)) {
        return true;
      } else if (deleteIntervalList.get(deleteCursor).getMax() < timestamp) {
        deleteCursor++;
      } else {
        return false;
      }
    }
    return false;
  }

  public void fillIsDeleted(long[] timestamp, boolean[] isDeleted) {
    for (int i = 0, n = timestamp.length; i < n; i++) {
      isDeleted[i] = isDeleted(timestamp[i]);
    }
  }

  public void fillIsDeleted(long[] timestamp, boolean[] isDeleted, boolean[] keepCurrentRow) {
    for (int i = 0, n = timestamp.length; i < n; i++) {
      if (keepCurrentRow[i]) {
        isDeleted[i] = isDeleted(timestamp[i]);
      }
    }
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public byte[] getBitmap() throws IOException {
    uncompressDataIfNecessary();
    return Arrays.copyOf(bitmap, bitmap.length);
  }
}
