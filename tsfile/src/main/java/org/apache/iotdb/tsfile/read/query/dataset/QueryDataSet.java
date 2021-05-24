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

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.io.IOException;
import java.util.List;

public abstract class QueryDataSet {

  protected List<Path> paths;
  protected List<TSDataType> dataTypes;

  protected int rowLimit = 0; // rowLimit > 0 means the LIMIT constraint exists
  protected int rowOffset = 0;
  protected int alreadyReturnedRowNum = 0;
  protected int fetchSize = 10000;
  protected boolean ascending;
  /*
   *  whether current data group has data for query.
   *  If not null(must be in cluster mode),
   *  we need to redirect the query to any data group which has some data to speed up query.
   */
  protected EndPoint endPoint = null;

  /** if any column is null, we don't need that row */
  protected boolean withoutAnyNull;

  /** Only if all columns are null, we don't need that row */
  protected boolean withoutAllNull;

  /** For redirect query. Need keep consistent with EndPoint in rpc.thrift. */
  public static class EndPoint {
    private String ip = null;
    private int port = 0;

    public EndPoint(String ip, int port) {
      this.ip = ip;
      this.port = port;
    }

    public EndPoint() {}

    public String getIp() {
      return ip;
    }

    public void setIp(String ip) {
      this.ip = ip;
    }

    public int getPort() {
      return port;
    }

    public void setPort(int port) {
      this.port = port;
    }

    @Override
    public String toString() {
      return "ip:port=" + ip + ":" + port;
    }
  }

  public QueryDataSet() {}

  public QueryDataSet(List<Path> paths, List<TSDataType> dataTypes) {
    initQueryDataSetFields(paths, dataTypes, true);
  }

  public QueryDataSet(List<Path> paths, List<TSDataType> dataTypes, boolean ascending) {
    initQueryDataSetFields(paths, dataTypes, ascending);
  }

  protected void initQueryDataSetFields(
      List<Path> paths, List<TSDataType> dataTypes, boolean ascending) {
    this.paths = paths;
    this.dataTypes = dataTypes;
    this.ascending = ascending;
  }

  public boolean hasNext() throws IOException {
    // proceed to the OFFSET row by skipping rows
    while (rowOffset > 0) {
      if (hasNextWithoutConstraint()) {
        RowRecord rowRecord = nextWithoutConstraint(); // DO NOT use next()
        // filter rows whose columns are null according to the rule
        if ((withoutAllNull && rowRecord.isAllNull())
            || (withoutAnyNull && rowRecord.hasNullField())) {
          continue;
        }
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

  /** This method is used for batch query, return RowRecord. */
  public RowRecord next() throws IOException {
    if (rowLimit > 0) {
      alreadyReturnedRowNum++;
    }
    return nextWithoutConstraint();
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
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

  public EndPoint getEndPoint() {
    return endPoint;
  }

  public void setEndPoint(EndPoint endPoint) {
    this.endPoint = endPoint;
  }

  public boolean isWithoutAnyNull() {
    return withoutAnyNull;
  }

  public void setWithoutAnyNull(boolean withoutAnyNull) {
    this.withoutAnyNull = withoutAnyNull;
  }

  public boolean isWithoutAllNull() {
    return withoutAllNull;
  }

  public void setWithoutAllNull(boolean withoutAllNull) {
    this.withoutAllNull = withoutAllNull;
  }

  public void decreaseAlreadyReturnedRowNum() {
    alreadyReturnedRowNum--;
  }
}
