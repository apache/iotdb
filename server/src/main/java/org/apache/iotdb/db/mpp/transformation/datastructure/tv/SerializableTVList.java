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

package org.apache.iotdb.db.mpp.transformation.datastructure.tv;

import org.apache.iotdb.db.mpp.transformation.datastructure.SerializableList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;

public abstract class SerializableTVList extends BatchData implements SerializableList {

  public static SerializableTVList newSerializableTVList(TSDataType dataType, long queryId) {
    SerializationRecorder recorder = new SerializationRecorder(queryId);
    switch (dataType) {
      case INT32:
        return new SerializableIntTVList(recorder);
      case INT64:
        return new SerializableLongTVList(recorder);
      case FLOAT:
        return new SerializableFloatTVList(recorder);
      case DOUBLE:
        return new SerializableDoubleTVList(recorder);
      case BOOLEAN:
        return new SerializableBooleanTVList(recorder);
      case TEXT:
        return new SerializableBinaryTVList(recorder);
      default:
        throw new UnSupportedDataTypeException(dataType.toString());
    }
  }

  protected static int calculateCapacity(TSDataType dataType, float memoryLimitInMB) {
    int size;
    switch (dataType) {
      case INT32:
        size = SerializableIntTVList.calculateCapacity(memoryLimitInMB);
        break;
      case INT64:
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
