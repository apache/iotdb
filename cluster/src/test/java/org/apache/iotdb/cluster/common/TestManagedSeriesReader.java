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

package org.apache.iotdb.cluster.common;

import java.util.NoSuchElementException;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class TestManagedSeriesReader implements ManagedSeriesReader, IReaderByTimestamp {

  private BatchData batchData;
  private boolean batchUsed = false;
  private boolean managedByQueryManager = false;
  private boolean hasRemaining = false;
  private TimeValuePair pairCache;
  private Filter filter;

  public TestManagedSeriesReader(BatchData batchData, Filter filter) {
    this.batchData = batchData;
    this.filter = filter;
  }

  @Override
  public boolean isManagedByQueryManager() {
    return managedByQueryManager;
  }

  @Override
  public void setManagedByQueryManager(boolean managedByQueryManager) {
    this.managedByQueryManager = managedByQueryManager;
  }

  @Override
  public boolean hasRemaining() {
    return hasRemaining;
  }

  @Override
  public void setHasRemaining(boolean hasRemaining) {
    this.hasRemaining = hasRemaining;
  }

  @Override
  public Object getValueInTimestamp(long timestamp) {
    while (batchData.hasCurrent()) {
      long currTime = batchData.currentTime();
      if (currTime == timestamp) {
        return batchData.currentValue();
      } else if (currTime > timestamp) {
        break;
      }
      batchData.next();
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    if (pairCache != null) {
      return true;
    }
    fetchPair();
    return pairCache != null;
  }

  private void fetchPair() {
    while (batchData.hasCurrent()) {
      long time = batchData.currentTime();
      Object value = batchData.currentValue();
      if (filter == null || filter.satisfy(time, value)) {
        pairCache = new TimeValuePair(time, TsPrimitiveType.getByType(batchData.getDataType(), value));
        batchData.next();
        break;
      }
      batchData.next();
    }
  }

  @Override
  public TimeValuePair next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    TimeValuePair ret = pairCache;
    pairCache = null;
    return ret;
  }

  @Override
  public TimeValuePair current() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return pairCache;
  }

  @Override
  public boolean hasNextBatch() {
    return !batchUsed;
  }

  @Override
  public BatchData nextBatch() {
    if (batchUsed) {
      throw new NoSuchElementException();
    }
    batchUsed = true;
    return batchData;
  }

  @Override
  public void close() {
    // nothing to be done
  }
}
