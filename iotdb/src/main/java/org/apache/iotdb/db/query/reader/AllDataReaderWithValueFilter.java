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
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * A value filter reader for read data source, including sequence data and unsequence data.
 */
public class AllDataReaderWithValueFilter extends AllDataReaderWithOptGlobalTimeFilter {

  private Filter filter;
  private boolean hasCachedValue;
  private TimeValuePair timeValuePair;

  /**
   * merge sequence reader, unsequence reader.
   */
  public AllDataReaderWithValueFilter(IBatchReader batchReader, IPointReader pointReader,
      Filter filter) {
    super(batchReader, pointReader);
    this.filter = filter;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (hasCachedValue) {
      return true;
    }
    while (super.hasNext()) {
      timeValuePair = super.next();
      if (filter.satisfy(timeValuePair.getTimestamp(), timeValuePair.getValue().getValue())) {
        hasCachedValue = true;
        return true;
      }
    }
    return false;
  }

  @Override
  public TimeValuePair next() throws IOException {
    if (hasCachedValue || hasNext()) {
      hasCachedValue = false;
      return timeValuePair;
    } else {
      throw new IOException("data reader is out of bound.");
    }
  }


  @Override
  public TimeValuePair current() throws IOException {
    return timeValuePair;
  }
}
