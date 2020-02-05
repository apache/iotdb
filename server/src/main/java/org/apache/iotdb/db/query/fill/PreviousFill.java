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
<<<<<<< HEAD
import org.apache.iotdb.tsfile.read.reader.IPointReader;
=======
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.IPointReader;
>>>>>>> 0b636dc926f73764a61fd208d956a0bbaaae75b7
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;

public class PreviousFill extends IFill {

  private long beforeRange;
  private BatchData batchData;

  public PreviousFill(TSDataType dataType, long queryTime, long beforeRange) {
    super(dataType, queryTime);
    this.beforeRange = beforeRange;
    batchData = new BatchData();
  }

  public PreviousFill(long beforeRange) {
    this.beforeRange = beforeRange;
  }

  @Override
  public IFill copy() {
    return new PreviousFill(dataType, queryTime, beforeRange);
  }

  @Override
  Filter constructFilter() {
    if (beforeRange == -1) {
      beforeRange = Long.MAX_VALUE;
    }
    // time in [queryTime - beforeRange, queryTime]
    return FilterFactory.and(TimeFilter.gtEq(queryTime - beforeRange),
        TimeFilter.ltEq(queryTime));
  }

  public long getBeforeRange() {
    return beforeRange;
  }

  @Override
  public IPointReader getFillResult() throws IOException {
    TimeValuePair beforePair = null;
    TimeValuePair cachedPair = null;
    while (batchData.hasCurrent() || allDataReader.hasNextBatch()) {
      if (!batchData.hasCurrent() && allDataReader.hasNextBatch()) {
        batchData = allDataReader.nextBatch();
      }
      cachedPair = new TimeValuePair(batchData.currentTime(), batchData.currentTsPrimitiveType());
      batchData.next();
      if (cachedPair.getTimestamp() <= queryTime) {
        beforePair = cachedPair;
      } else {
        break;
      }
    }

    if (beforePair != null) {
      beforePair.setTimestamp(queryTime);
    } else {
      beforePair = new TimeValuePair(queryTime, null);
    }
    return new TimeValuePairPointReader(beforePair);
  }
}
