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

package org.apache.iotdb.commons.schema.table.column;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public class IdColumnSchema extends TsTableColumnSchema {
  public IdColumnSchema(String columnName, TSDataType dataType) {
    super(columnName, dataType);
  }

  public IdColumnSchema(String columnName, TSDataType dataType, Map<String, String> props) {
    super(columnName, dataType, props);
  }

  @Override
  public TsTableColumnCategory getColumnCategory() {
    return TsTableColumnCategory.ID;
  }

  static IdColumnSchema deserialize(InputStream stream) throws IOException {
    String columnName = ReadWriteIOUtils.readString(stream);
    TSDataType dataType = ReadWriteIOUtils.readDataType(stream);
    Map<String, String> props = ReadWriteIOUtils.readMap(stream);
    return new IdColumnSchema(columnName, dataType, props);
  }

  static IdColumnSchema deserialize(ByteBuffer buffer) {
    String columnName = ReadWriteIOUtils.readString(buffer);
    TSDataType dataType = ReadWriteIOUtils.readDataType(buffer);
    Map<String, String> props = ReadWriteIOUtils.readMap(buffer);
    return new IdColumnSchema(columnName, dataType, props);
  }
}
