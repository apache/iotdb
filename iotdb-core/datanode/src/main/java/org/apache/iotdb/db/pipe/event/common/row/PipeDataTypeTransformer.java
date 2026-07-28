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

import org.apache.iotdb.db.utils.TypeServices;
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
    return tsDataType == null
        ? null
        : TypeServices.PIPE_DATA_TYPE_TRANSFORMER_SERVICE.call(
            org.apache.tsfile.read.common.type.Type.fromTsDataType(tsDataType));
  }

  private PipeDataTypeTransformer() {
    // util class
  }
}
