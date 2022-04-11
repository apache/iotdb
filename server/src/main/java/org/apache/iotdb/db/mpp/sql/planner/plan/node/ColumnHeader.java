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

package org.apache.iotdb.db.mpp.sql.planner.plan.node;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Objects;

public class ColumnHeader {

  private final String pathName;
  private String functionName;
  private final TSDataType dataType;

  public ColumnHeader(String pathName, TSDataType dataType) {
    this.pathName = pathName;
    this.dataType = dataType;
  }

  public ColumnHeader(String pathName, String functionName, TSDataType dataType) {
    this.pathName = pathName;
    this.functionName = functionName.toLowerCase();
    this.dataType = dataType;
  }

  public String getColumnName() {
    if (functionName != null) {
      return String.format("%s(%s)", functionName, pathName);
    }
    return pathName;
  }

  public TSDataType getColumnType() {
    return dataType;
  }

  public ColumnHeader replacePathWithMeasurement() {
    String measurement = null;
    try {
      measurement = new PartialPath(pathName).getMeasurement();
    } catch (IllegalPathException e) {
      e.printStackTrace();
    }
    if (functionName != null) {
      return new ColumnHeader(measurement, functionName, dataType);
    }
    return new ColumnHeader(measurement, dataType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ColumnHeader that = (ColumnHeader) o;
    return Objects.equals(pathName, that.pathName)
        && Objects.equals(functionName, that.functionName)
        && dataType == that.dataType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(pathName, functionName, dataType);
  }
}
