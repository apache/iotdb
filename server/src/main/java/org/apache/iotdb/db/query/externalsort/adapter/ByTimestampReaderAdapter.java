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
package org.apache.iotdb.db.query.externalsort.adapter;

import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;

/** This class is an adapter which makes IPointReader implement IReaderByTimestamp interface. */
public class ByTimestampReaderAdapter implements IReaderByTimestamp {

  private IPointReader pointReader;

  // only cache the first point that >= timestamp
  private boolean hasCached;
  private TimeValuePair pair;
  private long currentTime = Long.MIN_VALUE;

  public ByTimestampReaderAdapter(IPointReader pointReader) {
    this.pointReader = pointReader;
  }

  @Override
  public Object[] getValuesInTimestamps(long[] timestamps, int length) throws IOException {
    Object[] result = new Object[length];

    for (int i = 0; i < length; i++) {
      if (timestamps[i] < currentTime) {
        throw new IOException("time must be increasing when use ReaderByTimestamp");
      }
      currentTime = timestamps[i];
      // search cache
      if (hasCached && pair.getTimestamp() >= currentTime) {
        if (pair.getTimestamp() == currentTime) {
          hasCached = false;
          result[i] = pair.getValue().getValue();
        }
        continue;
      }
      // search reader
      while (pointReader.hasNextTimeValuePair()) {
        pair = pointReader.nextTimeValuePair();
        if (pair.getTimestamp() == currentTime) {
          result[i] = pair.getValue().getValue();
          break;
        } else if (pair.getTimestamp() > currentTime) {
          hasCached = true;
          result[i] = null;
          break;
        }
      }
    }
    return result;
  }
}
