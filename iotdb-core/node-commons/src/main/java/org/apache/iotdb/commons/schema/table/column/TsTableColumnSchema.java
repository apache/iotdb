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
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public abstract class TsTableColumnSchema {

  protected String columnName;

  protected TSDataType dataType;

  protected Map<String, String> props = null;

  TsTableColumnSchema(final String columnName, final TSDataType dataType) {
    this.columnName = columnName;
    this.dataType = dataType;
  }

  TsTableColumnSchema(
      final String columnName, final TSDataType dataType, final Map<String, String> props) {
    this.columnName = columnName;
    this.dataType = dataType;
    this.props = props;
  }

  public String getColumnName() {
    return columnName;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public Map<String, String> getProps() {
    if (Objects.isNull(props)) {
      props = new HashMap<>();
    }
    return props;
  }

  public abstract TsTableColumnCategory getColumnCategory();

  public IMeasurementSchema getMeasurementSchema() {
    return new MeasurementSchema(columnName, dataType);
  }

  void serialize(final OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(columnName, outputStream);
    ReadWriteIOUtils.write(dataType, outputStream);
    ReadWriteIOUtils.write(props, outputStream);
  }

  @Override
  public boolean equals(final Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName);
  }

  public void setDataType(final TSDataType dataType) {
    this.dataType = dataType;
  }

  public abstract TsTableColumnSchema copy();

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("columnName", columnName)
        .add("dataType", dataType)
        .add("props", props)
        .toString();
  }
}
