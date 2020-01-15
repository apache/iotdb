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
package org.apache.iotdb.tsfile.read.common;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class RowRecord {

  private long timestamp;
  private List<Field> fields;

  public RowRecord(long timestamp) {
    this.timestamp = timestamp;
    this.fields = new ArrayList<>();
  }

  public void addField(Field f) {
    this.fields.add(f);
  }

  public void addField(TsPrimitiveType tsPrimitiveType, TSDataType dataType) {
    this.fields.add(getField(tsPrimitiveType, dataType));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(timestamp);
    for (Field f : fields) {
      sb.append("\t");
      sb.append(f);
    }
    return sb.toString();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public List<Field> getFields() {
    return fields;
  }

  private Field getField(TsPrimitiveType tsPrimitiveType, TSDataType dataType) {
    if (tsPrimitiveType == null) {
      return new Field(null);
    }
    Field field = new Field(dataType);
    switch (dataType) {
      case INT32:
        field.setIntV(tsPrimitiveType.getInt());
        break;
      case INT64:
        field.setLongV(tsPrimitiveType.getLong());
        break;
      case FLOAT:
        field.setFloatV(tsPrimitiveType.getFloat());
        break;
      case DOUBLE:
        field.setDoubleV(tsPrimitiveType.getDouble());
        break;
      case BOOLEAN:
        field.setBoolV(tsPrimitiveType.getBoolean());
        break;
      case TEXT:
        field.setBinaryV(tsPrimitiveType.getBinary());
        break;
      default:
        throw new UnSupportedDataTypeException("UnSupported: " + dataType);
    }
    return field;
  }
}
