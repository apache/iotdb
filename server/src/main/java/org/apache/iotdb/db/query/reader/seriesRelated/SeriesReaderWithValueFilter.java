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
package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.reader.resourceRelated.SeqResourceIterateReader;
import org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceMergeReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * To read series data with value filter, this class extends {@link SeriesReaderWithoutValueFilter}
 * to implement <code>IPointReader</code> for the data.
 * <p>
 * Note that the difference between <code>SeriesReaderWithValueFilter</code> and
 * <code>SeriesReaderWithoutValueFilter</code> is that the former executes in the form of
 * filter(merge(filter(seqResource),unseqResource)) while the latter executes in the form of
 * merge(filter(seqResource),filter(unseqResource)) (when <code>pushdownUnseq</code> is True). More
 * see JIRA IOTDB-121. This difference is necessary to guarantee the correctness of the result.
 * <p>
 * This class is used by {@link org.apache.iotdb.db.query.timegenerator.EngineTimeGenerator}.
 */
public class SeriesReaderWithValueFilter extends SeriesReaderWithoutValueFilter {

  private Filter filter;
  private boolean hasCachedValue;
  private TimeValuePair timeValuePair;

  public SeriesReaderWithValueFilter(Path seriesPath, TSDataType dataType, Filter filter, QueryContext context)
      throws StorageEngineException, IOException {
    super(seriesPath, dataType, filter, context, false);
    this.filter = filter;
  }

  /**
   * for test
   */
  SeriesReaderWithValueFilter(IBatchReader seqResourceIterateReader,
      IBatchReader unseqResourceMergeReader, Filter filter) throws IOException {
    super(seqResourceIterateReader, unseqResourceMergeReader);
    this.filter = filter;
  }

//  @Override
//  public boolean hasNext() throws IOException {
//    if (hasCachedValue) {
//      return true;
//    }
//    while (super.hasNext()) {
//      timeValuePair = super.next();
//      if (filter.satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
//        hasCachedValue = true;
//        return true;
//      }
//    }
//    return false;
//  }
//
//  @Override
//  public TimeValuePair next() throws IOException {
//    if (hasCachedValue || hasNext()) {
//      hasCachedValue = false;
//      return timeValuePair;
//    } else {
//      throw new IOException("data reader is out of bound.");
//    }
//  }
}