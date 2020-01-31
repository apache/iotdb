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
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.IPointReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;


public class SeriesDataPointReader implements IPointReader {

  private SeriesDataRandomReader randomReader;
  private boolean hasCachedTimeValuePair;
  private BatchData batchData;
  private TimeValuePair timeValuePair;

  public SeriesDataPointReader(Path seriesPath, TSDataType dataType, Filter timeFilter,
      Filter valueFilter, QueryContext context, QueryDataSource dataSource) {
    randomReader = new SeriesDataRandomReader(seriesPath, dataType, context,
        dataSource.getSeqResources(), dataSource.getUnseqResources(), timeFilter, valueFilter);
  }


  private boolean hasNextOverlappedPage() throws IOException {
    while (randomReader.hasNextChunk()) {
      while (randomReader.hasNextPage()) {
        if (randomReader.hasNextOverlappedPage()) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean hasNextSatisfiedInCurrentBatch() {
    while (batchData != null && batchData.hasCurrent()) {
      timeValuePair = new TimeValuePair(batchData.currentTime(),
          batchData.currentTsPrimitiveType());
      hasCachedTimeValuePair = true;
      batchData.next();
      return true;
    }
    return false;
  }

  public TimeValuePair current() {
    return timeValuePair;
  }

  @Override
  public boolean hasNextTimeValuePair() throws IOException {
    if (hasCachedTimeValuePair) {
      return true;
    }

    if (hasNextSatisfiedInCurrentBatch()) {
      return true;
    }

    // has not cached timeValuePair
    while (hasNextOverlappedPage()) {
      batchData = randomReader.nextOverlappedPage();
      if (hasNextSatisfiedInCurrentBatch()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair nextTimeValuePair() throws IOException {
    if (hasCachedTimeValuePair || hasNextTimeValuePair()) {
      hasCachedTimeValuePair = false;
      return timeValuePair;
    } else {
      throw new IOException("no next data");
    }
  }

  @Override
  public TimeValuePair currentTimeValuePair() throws IOException {
    return null;
  }

  @Override
  public void close() throws IOException {
    randomReader.close();
  }
}
