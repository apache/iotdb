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

import java.io.IOException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.seriesRelated.SeriesReaderWithoutValueFilter;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

public abstract class IFill {

  long queryTime;
  TSDataType dataType;

  IPointReader allDataReader;

  public IFill(TSDataType dataType, long queryTime) {
    this.dataType = dataType;
    this.queryTime = queryTime;
  }

  public IFill() {
  }

  public abstract IFill copy(Path path);

  public abstract void constructReaders(Path path, QueryContext context)
      throws IOException, StorageEngineException;

  void constructReaders(Path path, QueryContext context, long beforeRange)
      throws IOException, StorageEngineException {
    Filter timeFilter = constructFilter(beforeRange);
    allDataReader = new SeriesReaderWithoutValueFilter(path, timeFilter, context);
  }

  public abstract IPointReader getFillResult() throws IOException;

  public TSDataType getDataType() {
    return this.dataType;
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  public void setQueryTime(long queryTime) {
    this.queryTime = queryTime;
  }

  private Filter constructFilter(long beforeRange) {
    // if the fill time range is not set, beforeRange will be set to -1.
    if (beforeRange == -1) {
      return null;
    }
    return TimeFilter.gtEq(queryTime - beforeRange);
  }

  class TimeValuePairPointReader implements IPointReader {

    private boolean isUsed;
    private TimeValuePair pair;

    public TimeValuePairPointReader(TimeValuePair pair) {
      this.pair = pair;
      this.isUsed = (pair == null);
    }

    @Override
    public boolean hasNext() {
      return !isUsed;
    }

    @Override
    public TimeValuePair next() {
      isUsed = true;
      return pair;
    }

    @Override
    public TimeValuePair current() {
      return pair;
    }

    @Override
    public void close() {
      // no need to close
    }
  }
}