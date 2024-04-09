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

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ColumnSchemaUtil {

  private ColumnSchemaUtil() {
    // do nothing
  }

  public static void serialize(ColumnSchema columnSchema, OutputStream outputStream)
      throws IOException {
    ReadWriteIOUtils.write(columnSchema.getColumnCategory().getValue(), outputStream);
    columnSchema.serialize(outputStream);
  }

  public static ColumnSchema deserialize(InputStream inputStream) throws IOException {
    return deserialize(ColumnCategory.deserialize(inputStream), inputStream);
  }

  private static ColumnSchema deserialize(ColumnCategory category, InputStream stream)
      throws IOException {
    switch (category) {
      case ID:
        return IdColumnSchema.deserialize(stream);
      case ATTRIBUTE:
        return AttributeColumnSchema.deserialize(stream);
      case TIME:
        return TimeColumnSchema.deserialize(stream);
      case MEASUREMENT:
        return MeasurementColumnSchema.deserialize(stream);
      default:
        throw new IllegalArgumentException();
    }
  }
}
