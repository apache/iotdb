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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class RowRecord {

  private final long timestamp;
  private final List<Field> fields;
  /** if any column is null, this field should be set to true; otherwise false */
  private boolean hasNullField = false;

  /** if any column is not null, this field should be set to false; otherwise true */
  private boolean allNull = true;

  public RowRecord(long timestamp) {
    this.timestamp = timestamp;
    this.fields = new ArrayList<>();
  }

  public RowRecord(long timestamp, List<Field> fields) {
    this.timestamp = timestamp;
    this.fields = fields;
    for (Field field : fields) {
      if (field == null || field.getDataType() == null) {
        hasNullField = true;
      } else {
        allNull = false;
      }
    }
  }

  public void addField(Field f) {
    this.fields.add(f);
    if (f == null || f.getDataType() == null) {
      hasNullField = true;
    } else {
      allNull = false;
    }
  }

  public void addField(Object value, TSDataType dataType) {
    this.fields.add(Field.getField(value, dataType));
    if (value == null || dataType == null) {
      hasNullField = true;
    } else {
      allNull = false;
    }
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

  public List<Field> getFields() {
    return fields;
  }

  public boolean hasNullField() {
    return hasNullField;
  }

  public boolean isAllNull() {
    return allNull;
  }

  public void resetNullFlag() {
    hasNullField = false;
    allNull = true;
  }
}
