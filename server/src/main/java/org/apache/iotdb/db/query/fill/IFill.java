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

package org.apache.iotdb.db.query.fill;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.UnSupportedFillTypeException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.series.SeriesRawDataBatchReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;

import java.io.IOException;

public abstract class IFill {

  long queryTime;
  TSDataType dataType;

  IBatchReader allDataReader;

  public IFill(TSDataType dataType, long queryTime) {
    this.dataType = dataType;
    this.queryTime = queryTime;
  }

  public IFill() {
  }

  public abstract IFill copy();

  public void constructReaders(Path path, QueryContext context)
      throws StorageEngineException {
    Filter timeFilter = constructFilter();
    allDataReader = new SeriesRawDataBatchReader(path, dataType, context,
        QueryResourceManager.getInstance().getQueryDataSource(path, context, timeFilter),
        timeFilter, null, null);
  }

  public void setAllDataReader(IBatchReader allDataReader) {
    this.allDataReader = allDataReader;
  }

  public Filter getFilter() {
    return constructFilter();
  }

  public abstract TimeValuePair getFillResult() throws IOException, UnSupportedFillTypeException;

  public TSDataType getDataType() {
    return this.dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public void setQueryTime(long queryTime) {
    this.queryTime = queryTime;
  }

  abstract Filter constructFilter();
}