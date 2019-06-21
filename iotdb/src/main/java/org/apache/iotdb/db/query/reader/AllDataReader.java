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

package org.apache.iotdb.db.query.reader;

import java.io.IOException;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TimeValuePairUtils;
import org.apache.iotdb.tsfile.read.common.BatchData;

/**
 * include all data of a series: sequence data and unsequence data.
 */
public class AllDataReader implements IPointReader {

  private IBatchReader batchReader;
  private IPointReader pointReader;

  private boolean hasCachedBatchData;
  private BatchData batchData;

  /**
   * merge sequence reader, unsequence reader.
   */
  public AllDataReader(IBatchReader batchReader, IPointReader pointReader) {
    this.batchReader = batchReader;
    this.pointReader = pointReader;

    this.hasCachedBatchData = false;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasNextInBatchDataOrBatchReader()) {
      return true;
    }
    // has value in pointReader
    return pointReader != null && pointReader.hasNext();
  }

  @Override
  public TimeValuePair next() throws IOException {

    // has next in both batch reader and point reader
    if (hasNextInBothReader()) {
      long timeInPointReader = pointReader.current().getTimestamp();
      long timeInBatchData = batchData.currentTime();
      if (timeInPointReader > timeInBatchData) {
        TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(batchData);
        batchData.next();
        return timeValuePair;
      } else if (timeInPointReader == timeInBatchData) {
        batchData.next();
        return pointReader.next();
      } else {
        return pointReader.next();
      }
    }

    // only has next in batch reader
    if (hasNextInBatchDataOrBatchReader()) {
      TimeValuePair timeValuePair = TimeValuePairUtils.getCurrentTimeValuePair(batchData);
      batchData.next();
      return timeValuePair;
    }

    // only has next in point reader
    if (pointReader != null && pointReader.hasNext()) {
      return pointReader.next();
    }
    return null;
  }

  /**
   * judge if has next in both batch record and pointReader.
   */
  private boolean hasNextInBothReader() throws IOException {
    if (!hasNextInBatchDataOrBatchReader()) {
      return false;
    }
    return pointReader != null && pointReader.hasNext();
  }

  /**
   * judge if has next in batch record, either in batch data or in batch reader.
   */
  private boolean hasNextInBatchDataOrBatchReader() throws IOException {
    // has value in batchData
    if (hasCachedBatchData && batchData.hasNext()) {
      return true;
    } else {
      hasCachedBatchData = false;
    }

    // has value in batchReader
    while (batchReader.hasNext()) {
      batchData = batchReader.nextBatch();
      if (batchData.hasNext()) {
        hasCachedBatchData = true;
        return true;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair current() throws IOException {
    throw new IOException("current() in AllDataReader is an empty method.");
  }

  @Override
  public void close() throws IOException {
    batchReader.close();
    pointReader.close();
  }
}
