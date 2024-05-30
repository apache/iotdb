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

package org.apache.iotdb.db.queryengine.transformation.datastructure.tv;

import org.apache.iotdb.db.queryengine.transformation.datastructure.SerializableList;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.write.UnSupportedDataTypeException;

public abstract class SerializableTVList extends BatchData implements SerializableList {

  public static SerializableTVList newSerializableTVList(TSDataType dataType, String queryId) {
    SerializationRecorder recorder = new SerializationRecorder(queryId);
    switch (dataType) {
      case INT32:
      case DATE:
        return new SerializableIntTVList(recorder);
      case INT64:
      case TIMESTAMP:
        return new SerializableLongTVList(recorder);
      case FLOAT:
        return new SerializableFloatTVList(recorder);
      case DOUBLE:
        return new SerializableDoubleTVList(recorder);
      case BOOLEAN:
        return new SerializableBooleanTVList(recorder);
      case TEXT:
      case STRING:
      case BLOB:
        return new SerializableBinaryTVList(recorder);
      default:
        throw new UnSupportedDataTypeException(dataType.toString());
    }
  }

  protected static int calculateCapacity(TSDataType dataType, float memoryLimitInMB) {
    int size;
    switch (dataType) {
      case INT32:
      case DATE:
        size = SerializableIntTVList.calculateCapacity(memoryLimitInMB);
        break;
      case INT64:
      case TIMESTAMP:
        size = SerializableLongTVList.calculateCapacity(memoryLimitInMB);
        break;
      case FLOAT:
        size = SerializableFloatTVList.calculateCapacity(memoryLimitInMB);
        break;
      case DOUBLE:
        size = SerializableDoubleTVList.calculateCapacity(memoryLimitInMB);
        break;
      case BOOLEAN:
        size = SerializableBooleanTVList.calculateCapacity(memoryLimitInMB);
        break;
      case TEXT:
      case STRING:
      case BLOB:
        size =
            SerializableBinaryTVList.calculateCapacity(
                memoryLimitInMB, SerializableList.INITIAL_BYTE_ARRAY_LENGTH_FOR_MEMORY_CONTROL);
        break;
      default:
        throw new UnSupportedDataTypeException(dataType.toString());
    }

    if (size <= 0) {
      throw new RuntimeException("Memory is not enough for current query.");
    }
    return size;
  }

  protected final SerializationRecorder serializationRecorder;

  protected SerializableTVList(TSDataType type, SerializationRecorder serializationRecorder) {
    super(type);
    this.serializationRecorder = serializationRecorder;
  }

  @Override
  public SerializationRecorder getSerializationRecorder() {
    return serializationRecorder;
  }

  @Override
  public void init() {
    init(getDataType());
  }
}
