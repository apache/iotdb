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
package org.apache.iotdb.tsfile.read.reader.page;

import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.BatchDataFactory;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

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

  private void splitDataToBitmapAndValue(ByteBuffer pageData) {
    if (!pageData.hasRemaining()) { // Empty Page
      return;
    }
    this.size = ReadWriteIOUtils.readInt(pageData);
    this.bitmap = new byte[(size + 7) / 8];
    pageData.get(bitmap);
    this.valueBuffer = pageData.slice();
  }

  /**
   * return a BatchData with the corresponding timeBatch, the BatchData's dataType is same as this
   * sub sensor
   */
  public BatchData nextBatch(long[] timeBatch, boolean ascending, Filter filter) {
    BatchData pageData = BatchDataFactory.createBatchData(dataType, ascending, false);
    for (int i = 0; i < timeBatch.length; i++) {
      if (((bitmap[i / 8] & 0xFF) & (MASK >>> (i % 8))) == 0) {
        continue;
      }
      long timestamp = timeBatch[i];
      switch (dataType) {
        case BOOLEAN:
          boolean aBoolean = valueDecoder.readBoolean(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBoolean))) {
            pageData.putBoolean(timestamp, aBoolean);
          }
          break;
        case INT32:
          int anInt = valueDecoder.readInt(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, anInt))) {
            pageData.putInt(timestamp, anInt);
          }
          break;
        case INT64:
          long aLong = valueDecoder.readLong(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aLong))) {
            pageData.putLong(timestamp, aLong);
          }
          break;
        case FLOAT:
          float aFloat = valueDecoder.readFloat(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aFloat))) {
            pageData.putFloat(timestamp, aFloat);
          }
          break;
        case DOUBLE:
          double aDouble = valueDecoder.readDouble(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aDouble))) {
            pageData.putDouble(timestamp, aDouble);
          }
          break;
        case TEXT:
          Binary aBinary = valueDecoder.readBinary(valueBuffer);
          if (!isDeleted(timestamp) && (filter == null || filter.satisfy(timestamp, aBinary))) {
            pageData.putBinary(timestamp, aBinary);
          }
          break;
        default:
          throw new UnSupportedDataTypeException(String.valueOf(dataType));
      }
    }
    return pageData.flip();
  }

  public TsPrimitiveType nextValue(long timestamp, int timeIndex) {
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
        int anInt = valueDecoder.readInt(valueBuffer);
        if (!isDeleted(timestamp)) {
          resultValue = new TsPrimitiveType.TsInt(anInt);
        }
        break;
      case INT64:
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
  public TsPrimitiveType[] nextValueBatch(long[] timeBatch) {
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
          int anInt = valueDecoder.readInt(valueBuffer);
          if (!isDeleted(timeBatch[i])) {
            valueBatch[i] = new TsPrimitiveType.TsInt(anInt);
          }
          break;
        case INT64:
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
      int readEndIndex,
      ColumnBuilder columnBuilder,
      boolean[] keepCurrentRow,
      boolean[] isDeleted) {
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

  public Statistics getStatistics() {
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

  protected boolean isDeleted(long timestamp) {
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

  public TSDataType getDataType() {
    return dataType;
  }

  public byte[] getBitmap() {
    return Arrays.copyOf(bitmap, bitmap.length);
  }
}
