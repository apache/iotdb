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
package org.apache.iotdb.tsfile.read.query.dataset;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;

public abstract class QueryDataSet {

  protected List<Path> paths;
  protected List<TSDataType> dataTypes;

  protected int rowLimit = 0; // rowLimit > 0 means the LIMIT constraint exists
  protected int rowOffset = 0;
  protected int alreadyReturnedRowNum = 0;
  protected boolean ascending;

  public QueryDataSet() {
  }

  public QueryDataSet(List<Path> paths, List<TSDataType> dataTypes) {
    initQueryDataSetFields(paths, dataTypes, true);
  }

  public QueryDataSet(List<Path> paths, List<TSDataType> dataTypes, boolean ascending) {
    initQueryDataSetFields(paths, dataTypes, ascending);
  }

  protected void initQueryDataSetFields(List<Path> paths, List<TSDataType> dataTypes, boolean ascending) {
    this.paths = paths;
    this.dataTypes = dataTypes;
    this.ascending = ascending;
  }

  public boolean hasNext() throws IOException {
    // proceed to the OFFSET row by skipping rows
    while (rowOffset > 0) {
      if (hasNextWithoutConstraint()) {
        nextWithoutConstraint(); // DO NOT use next()
        rowOffset--;
      } else {
        return false;
      }
    }

    // make sure within the LIMIT constraint if exists
    if (rowLimit > 0 && alreadyReturnedRowNum >= rowLimit) {
      return false;
    }

    return hasNextWithoutConstraint();
  }

  public abstract boolean hasNextWithoutConstraint() throws IOException;

  /**
   * This method is used for batch query, return RowRecord.
   */
  public RowRecord next() throws IOException {
    if (rowLimit > 0) {
      alreadyReturnedRowNum++;
    }
    return nextWithoutConstraint();
  }

  public abstract RowRecord nextWithoutConstraint() throws IOException;

  public List<Path> getPaths() {
    return paths;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public int getRowLimit() {
    return rowLimit;
  }

  public void setRowLimit(int rowLimit) {
    this.rowLimit = rowLimit;
  }

  public int getRowOffset() {
    return rowOffset;
  }

  public void setRowOffset(int rowOffset) {
    this.rowOffset = rowOffset;
  }

  public boolean hasLimit() {
    return rowLimit > 0;
  }
}
