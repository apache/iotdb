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

package org.apache.iotdb.db.mpp.common.header;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** The header of query result dataset. */
public class DatasetHeader {

  // column names, data types and aliases of result dataset
  private final List<ColumnHeader> columnHeaders;

  // indicate whether the result dataset contain timestamp column
  private final boolean isIgnoreTimestamp;

  // map from
  private Map<ColumnHeader, Integer> columnToTsBlockIndexMap;

  public DatasetHeader(List<ColumnHeader> columnHeaders, boolean isIgnoreTimestamp) {
    this.columnHeaders = columnHeaders;
    this.isIgnoreTimestamp = isIgnoreTimestamp;
  }

  public List<ColumnHeader> getColumnHeaders() {
    return columnHeaders;
  }

  public boolean isIgnoreTimestamp() {
    return isIgnoreTimestamp;
  }

  public Map<ColumnHeader, Integer> getColumnToTsBlockIndexMap() {
    return columnToTsBlockIndexMap;
  }

  public void setColumnToTsBlockIndexMap(Map<ColumnHeader, Integer> columnToTsBlockIndexMap) {
    this.columnToTsBlockIndexMap = columnToTsBlockIndexMap;
  }

  public List<String> getRespColumns() {
    return new ArrayList<>();
  }

  public List<TSDataType> getRespDataTypeList() {
    return new ArrayList<>();
  }

  public List<Byte> getRespAliasColumns() {
    return new ArrayList<>();
  }
}
