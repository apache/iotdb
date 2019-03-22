/**
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
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.factory.SeriesReaderFactory;
import org.apache.iotdb.db.query.reader.AllDataReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
import org.apache.iotdb.db.query.reader.sequence.SequenceDataReader;
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

  public abstract void constructReaders(QueryDataSource queryDataSource, QueryContext context)
      throws IOException;

  void constructReaders(QueryDataSource queryDataSource, QueryContext context, long beforeRange)
      throws IOException {
    Filter timeFilter = constructFilter(beforeRange);
    // sequence reader for sealed tsfile, unsealed tsfile, memory
    SequenceDataReader sequenceReader = new SequenceDataReader(queryDataSource.getSeqDataSource(),
        timeFilter, context, false);

    // unseq reader for all chunk groups in unSeqFile, memory
    PriorityMergeReader unSeqMergeReader = SeriesReaderFactory.getInstance()
        .createUnSeqMergeReader(queryDataSource.getOverflowSeriesDataSource(), timeFilter);

    allDataReader = new AllDataReader(sequenceReader, unSeqMergeReader);
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
    //If the fill time range is not set, beforeRange will be set to -1.
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
    public boolean hasNext() throws IOException {
      return !isUsed;
    }

    @Override
    public TimeValuePair next() throws IOException {
      isUsed = true;
      return pair;
    }

    @Override
    public TimeValuePair current() throws IOException {
      return pair;
    }

    @Override
    public void close() throws IOException {

    }
  }
}