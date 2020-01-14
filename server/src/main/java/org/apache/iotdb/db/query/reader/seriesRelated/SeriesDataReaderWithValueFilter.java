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
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;


public class SeriesDataReaderWithValueFilter extends SeriesDataReaderWithoutValueFilter {

  private final Filter valueFilter;
  private boolean hasCachedTimeValuePair;
  private BatchData batchData;
  private TimeValuePair timeValuePair;

  public SeriesDataReaderWithValueFilter(Path seriesPath, TSDataType dataType, Filter valueFilter,
      QueryContext context) throws StorageEngineException, IOException {
    super(seriesPath, dataType, null, context);
    this.valueFilter = valueFilter;
  }

  @Override
  protected boolean satisfyFilter(Statistics statistics) {
    return false;
  }

  public boolean hasNext() throws IOException {
    if (hasCachedTimeValuePair) {
      return true;
    }

    if (hasNextSatisfiedInCurrentBatch()) {
      return true;
    }

    // has not cached timeValuePair
    while (hasNextBatch()) {
      batchData = super.nextBatch();
      if (hasNextSatisfiedInCurrentBatch()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean hasNextBatch() throws IOException {
    while (hasNextChunk()) {
      while (hasNextPage()) {
        if (super.hasNextBatch()) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean hasNextSatisfiedInCurrentBatch() {
    while (batchData != null && batchData.hasCurrent()) {
      if (valueFilter.satisfy(batchData.currentTime(), batchData.currentValue())) {
        timeValuePair = new TimeValuePair(batchData.currentTime(),
            batchData.currentTsPrimitiveType());
        hasCachedTimeValuePair = true;
        batchData.next();
        return true;
      }
      batchData.next();
    }
    return false;
  }

  public TimeValuePair next() throws IOException {
    if (hasCachedTimeValuePair || hasNext()) {
      hasCachedTimeValuePair = false;
      return timeValuePair;
    } else {
      throw new IOException("no next data");
    }
  }

  public TimeValuePair current() {
    return timeValuePair;
  }
}
