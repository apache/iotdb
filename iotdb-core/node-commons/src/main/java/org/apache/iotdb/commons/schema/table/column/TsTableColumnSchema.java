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
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;

public abstract class TsTableColumnSchema {

  protected String columnName;

  protected TSDataType dataType;

  protected Map<String, String> props = null;

  TsTableColumnSchema(String columnName, TSDataType dataType) {
    this.columnName = columnName;
    this.dataType = dataType;
  }

  TsTableColumnSchema(String columnName, TSDataType dataType, Map<String, String> props) {
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

  public abstract TsTableColumnCategory getColumnCategory();

  void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(columnName, outputStream);
    ReadWriteIOUtils.write(dataType, outputStream);
    ReadWriteIOUtils.write(props, outputStream);
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columnName);
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }
}
