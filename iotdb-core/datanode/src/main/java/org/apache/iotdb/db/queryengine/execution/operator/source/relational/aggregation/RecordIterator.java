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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation;

import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.utils.ObjectTypeUtils;
import org.apache.iotdb.udf.api.relational.access.Record;
import org.apache.iotdb.udf.api.type.Type;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.ObjectType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;

import java.io.File;
import java.time.LocalDate;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.apache.tsfile.read.common.type.BlobType.BLOB;

public class RecordIterator implements Iterator<Record> {

  public static final String OBJECT_ERR_MSG =
      "OBJECT Type only support getString, getObjectFile, objectLength and readObject";

  private final List<Column> childrenColumns;
  private final List<org.apache.tsfile.read.common.type.Type> dataTypes;
  private final int positionCount;
  protected int currentIndex;

  public RecordIterator(
      List<Column> childrenColumns,
      List<org.apache.tsfile.read.common.type.Type> dataTypes,
      int positionCount) {
    this.childrenColumns = childrenColumns;
    this.dataTypes = dataTypes;
    this.positionCount = positionCount;
    if (childrenColumns.size() != dataTypes.size()) {
      throw new IllegalArgumentException(
          "The size of childrenColumns and dataTypes should be the same.");
    }
  }

  protected int getCurrentIndex() {
    return currentIndex++;
  }

  @Override
  public boolean hasNext() {
    return currentIndex < positionCount;
  }

  @Override
  public Record next() {
    return new RecordImpl(childrenColumns, dataTypes, getCurrentIndex());
  }

  private static class RecordImpl implements Record {

    private final List<Column> childrenColumns;
    private final List<org.apache.tsfile.read.common.type.Type> dataTypes;
    private final int index;

    private RecordImpl(
        List<Column> childrenColumns,
        List<org.apache.tsfile.read.common.type.Type> dataTypes,
        int index) {
      this.childrenColumns = childrenColumns;
      this.dataTypes = dataTypes;
      this.index = index;
    }

    @Override
    public int getInt(int columnIndex) {
      return childrenColumns.get(columnIndex).getInt(index);
    }

    @Override
    public long getLong(int columnIndex) {
      return childrenColumns.get(columnIndex).getLong(index);
    }

    @Override
    public float getFloat(int columnIndex) {
      return childrenColumns.get(columnIndex).getFloat(index);
    }

    @Override
    public double getDouble(int columnIndex) {
      return childrenColumns.get(columnIndex).getDouble(index);
    }

    @Override
    public boolean getBoolean(int columnIndex) {
      return childrenColumns.get(columnIndex).getBoolean(index);
    }

    @Override
    public Binary getBinary(int columnIndex) {
      org.apache.tsfile.read.common.type.Type type = dataTypes.get(columnIndex);
      if (type == ObjectType.OBJECT) {
        throw new UnsupportedOperationException(OBJECT_ERR_MSG);
      }
      return getBinarySafely(columnIndex);
    }

    private Binary getBinarySafely(int columnIndex) {
      return childrenColumns.get(columnIndex).getBinary(index);
    }

    @Override
    public String getString(int columnIndex) {
      Binary binary = childrenColumns.get(columnIndex).getBinary(index);
      org.apache.tsfile.read.common.type.Type type = dataTypes.get(columnIndex);
      if (type == ObjectType.OBJECT) {
        return BytesUtils.parseObjectByteArrayToString(binary.getValues());
      } else if (type == BLOB) {
        return BytesUtils.parseBlobByteArrayToString(binary.getValues());
      } else {
        return binary.getStringValue(TSFileConfig.STRING_CHARSET);
      }
    }

    @Override
    public LocalDate getLocalDate(int columnIndex) {
      return DateUtils.parseIntToLocalDate(childrenColumns.get(columnIndex).getInt(index));
    }

    @Override
    public Object getObject(int columnIndex) {
      org.apache.tsfile.read.common.type.Type type = dataTypes.get(columnIndex);
      if (type == ObjectType.OBJECT) {
        throw new UnsupportedOperationException(OBJECT_ERR_MSG);
      }
      return childrenColumns.get(columnIndex).getObject(index);
    }

    @Override
    public Optional<File> getObjectFile(int columnIndex) {
      if (getDataType(columnIndex) != Type.OBJECT) {
        throw new UnsupportedOperationException("current column is not object column");
      }
      return ObjectTypeUtils.getObjectPathFromBinary(getBinarySafely(columnIndex));
    }

    @Override
    public long objectLength(int columnIndex) {
      if (getDataType(columnIndex) != Type.OBJECT) {
        throw new UnsupportedOperationException("current column is not object column");
      }
      Binary binary = getBinarySafely(columnIndex);
      return ObjectTypeUtils.getObjectLength(binary);
    }

    @Override
    public Binary readObject(int columnIndex, long offset, int length) {
      if (getDataType(columnIndex) != Type.OBJECT) {
        throw new UnsupportedOperationException("current column is not object column");
      }
      Binary binary = getBinarySafely(columnIndex);
      return new Binary(ObjectTypeUtils.readObjectContent(binary, offset, length, true).array());
    }

    @Override
    public Binary readObject(int columnIndex) {
      return readObject(columnIndex, 0L, -1);
    }

    @Override
    public Type getDataType(int columnIndex) {
      return UDFDataTypeTransformer.transformReadTypeToUDFDataType(dataTypes.get(columnIndex));
    }

    @Override
    public boolean isNull(int columnIndex) {
      return childrenColumns.get(columnIndex).isNull(index);
    }

    @Override
    public int size() {
      return childrenColumns.size();
    }
  }
}
