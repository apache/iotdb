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
package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;
import java.util.List;

/**
 * the input data set of an UDAF query. It accepts any query results as input instead of raw
 * timeseries data
 */
public class UDFInputDataSet implements IUDFInputDataSet {

  private final QueryDataSet dataSet;

  public UDFInputDataSet(QueryDataSet dataSet) {
    this.dataSet = dataSet;
  }

  @Override
  public List<TSDataType> getDataTypes() {
    return dataSet.getDataTypes();
  }

  @Override
  public boolean hasNextRowInObjects() throws IOException {
    return dataSet.hasNextWithoutConstraint();
  }

  @Override
  public Object[] nextRowInObjects() throws IOException {
    Object[] nextRow = new Object[dataSet.getColumnNum() + 1];
    RowRecord r = dataSet.next();
    for (int i = 0; i < dataSet.getColumnNum(); i++) {
      nextRow[i] = r.getFields().get(i).getObjectValue(dataSet.getDataTypes().get(i));
    }
    nextRow[dataSet.getColumnNum()] = r.getTimestamp();
    return nextRow;
  }
}
