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

package org.apache.tsfile.read.query.executor.task;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.query.executor.TableQueryExecutor.ColumnMapping;

import java.util.List;

public class DeviceQueryTask {
  private final IDeviceID deviceID;
  private final List<String> columnNames;
  private final ColumnMapping columnMapping;
  private final MetadataIndexNode indexRoot;
  private final TableSchema tableSchema;

  public DeviceQueryTask(
      IDeviceID deviceID,
      List<String> columnNames,
      ColumnMapping columnMapping,
      MetadataIndexNode indexRoot,
      TableSchema tableSchema) {
    this.deviceID = deviceID;
    this.columnNames = columnNames;
    this.columnMapping = columnMapping;
    this.indexRoot = indexRoot;
    this.tableSchema = tableSchema;
  }

  public IDeviceID getDeviceID() {
    return deviceID;
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public ColumnMapping getColumnMapping() {
    return columnMapping;
  }

  public MetadataIndexNode getIndexRoot() {
    return indexRoot;
  }

  public TableSchema getTableSchema() {
    return tableSchema;
  }

  @Override
  public String toString() {
    return "DeviceQueryTask{" + "deviceID=" + deviceID + ", columnNames=" + columnNames + '}';
  }
}
