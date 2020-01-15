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
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.common.Path;

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
  public IFill copy(Path path) {
    return new PreviousFill(dataType, queryTime, beforeRange);
  }

  @Override
  public void constructReaders(Path path, QueryContext context)
      throws IOException, StorageEngineException {
    super.constructReaders(path, context, beforeRange);
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
      if (batchData == null) {
        break;
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
