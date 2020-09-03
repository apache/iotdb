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

package org.apache.iotdb.db.query.udf.datastructure;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

public interface SerializableTVList extends SerializableList {

  static int calculateCapacity(TSDataType dataType, float memoryLimitInMB)
      throws QueryProcessException {
    final int MIN_OBJECT_HEADER_SIZE = 8;
    final int MIN_ARRAY_HEADER_SIZE = MIN_OBJECT_HEADER_SIZE + 4;
    int size;
    memoryLimitInMB /= 2; // half for SerializableTVList and half for its serialization
    switch (dataType) {
      case INT32: // times + values
        size = (int) (memoryLimitInMB / (TSFileConfig.ARRAY_CAPACITY_THRESHOLD
            * (ReadWriteIOUtils.LONG_LEN + ReadWriteIOUtils.INT_LEN)));
        break;
      case INT64: // times + values
        size = (int) (memoryLimitInMB / (TSFileConfig.ARRAY_CAPACITY_THRESHOLD
            * (ReadWriteIOUtils.LONG_LEN + ReadWriteIOUtils.LONG_LEN)));
        break;
      case FLOAT: // times + values
        size = (int) (memoryLimitInMB / (TSFileConfig.ARRAY_CAPACITY_THRESHOLD
            * (ReadWriteIOUtils.LONG_LEN + ReadWriteIOUtils.FLOAT_LEN)));
        break;
      case DOUBLE: // times + values
        size = (int) (memoryLimitInMB / (TSFileConfig.ARRAY_CAPACITY_THRESHOLD
            * (ReadWriteIOUtils.LONG_LEN + ReadWriteIOUtils.DOUBLE_LEN)));
        break;
      case BOOLEAN: // times + values
        size = (int) (memoryLimitInMB / (TSFileConfig.ARRAY_CAPACITY_THRESHOLD
            * (ReadWriteIOUtils.LONG_LEN + ReadWriteIOUtils.BOOLEAN_LEN)));
        break;
      case TEXT: // times + offsets + values
        size = (int) (memoryLimitInMB / (TSFileConfig.ARRAY_CAPACITY_THRESHOLD
            * (ReadWriteIOUtils.LONG_LEN
            + ReadWriteIOUtils.LONG_LEN
            + MIN_OBJECT_HEADER_SIZE // Binary header
            + ReadWriteIOUtils.INT_LEN + MIN_ARRAY_HEADER_SIZE // Binary.values
            + SerializableList.BINARY_AVERAGE_LENGTH_FOR_MEMORY_CONTROL)));
        break;
      default:
        throw new UnSupportedDataTypeException(dataType.toString());
    }
    if (size <= 0) {
      throw new QueryProcessException("Memory is not enough for current query.");
    }
    return size * TSFileConfig.ARRAY_CAPACITY_THRESHOLD;
  }

  static BatchData newSerializableTVList(TSDataType dataType, long queryId, String uniqueId,
      int index) {
    SerializationRecorder recorder = new SerializationRecorder(queryId, uniqueId, index);
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
}
