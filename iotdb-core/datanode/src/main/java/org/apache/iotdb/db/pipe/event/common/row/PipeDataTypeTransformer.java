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

package org.apache.iotdb.db.pipe.event.common.row;

import org.apache.iotdb.pipe.api.type.Type;

import org.apache.tsfile.enums.TSDataType;

import java.util.List;
import java.util.stream.Collectors;

/** Transform between {@link TSDataType} and {@link org.apache.iotdb.pipe.api.type.Type}. */
public class PipeDataTypeTransformer {

  public static List<Type> transformToPipeDataTypeList(final List<TSDataType> tsDataTypeList) {
    return tsDataTypeList == null
        ? null
        : tsDataTypeList.stream()
            .map(PipeDataTypeTransformer::transformToPipeDataType)
            .collect(Collectors.toList());
  }

  public static Type transformToPipeDataType(final TSDataType tsDataType) {
    return tsDataType == null ? null : getPipeDataType(tsDataType.getType());
  }

  private static Type getPipeDataType(final byte type) {
    switch (type) {
      case 0:
        return Type.BOOLEAN;
      case 1:
        return Type.INT32;
      case 2:
        return Type.INT64;
      case 3:
        return Type.FLOAT;
      case 4:
        return Type.DOUBLE;
      case 5:
        return Type.TEXT;
      case 8:
        return Type.TIMESTAMP;
      case 9:
        return Type.DATE;
      case 10:
        return Type.BLOB;
      case 11:
        return Type.STRING;
      default:
        throw new IllegalArgumentException("Invalid input: " + type);
    }
  }

  private PipeDataTypeTransformer() {
    // util class
  }
}
