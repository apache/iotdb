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
import java.util.HashMap;
import java.util.Map;

public class TimeColumnSchema extends TsTableColumnSchema {
  public TimeColumnSchema(final String columnName, final TSDataType dataType) {
    super(columnName, dataType);
  }

  public TimeColumnSchema(
      final String columnName, final TSDataType dataType, final Map<String, String> props) {
    super(columnName, dataType, props);
  }

  @Override
  public TsTableColumnCategory getColumnCategory() {
    return TsTableColumnCategory.TIME;
  }

  static TimeColumnSchema deserialize(final InputStream stream) throws IOException {
    final String columnName = ReadWriteIOUtils.readString(stream);
    final TSDataType dataType = ReadWriteIOUtils.readDataType(stream);
    final Map<String, String> props = ReadWriteIOUtils.readMap(stream);
    return new TimeColumnSchema(columnName, dataType, props);
  }

  static TimeColumnSchema deserialize(final ByteBuffer buffer) {
    final String columnName = ReadWriteIOUtils.readString(buffer);
    final TSDataType dataType = ReadWriteIOUtils.readDataType(buffer);
    final Map<String, String> props = ReadWriteIOUtils.readMap(buffer);
    return new TimeColumnSchema(columnName, dataType, props);
  }

  @Override
  public TsTableColumnSchema copy() {
    return new TimeColumnSchema(columnName, dataType, props == null ? null : new HashMap<>(props));
  }
}
