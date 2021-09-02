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

package org.apache.iotdb.db.metadata.lastCache.container.value;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class UnaryLastCacheValue implements ILastCacheValue {

  private static final String INDEX_OPERATION_ON_MONAD_EXCEPTION =
      "Cannot operate data on any index but 0 on MonadLastCacheValue";

  private long timestamp;

  private TsPrimitiveType value;

  public UnaryLastCacheValue(long timestamp, TsPrimitiveType value) {
    this.timestamp = timestamp;
    this.value = value;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public void setValue(TsPrimitiveType value) {
    this.value = value;
  }

  @Override
  public TimeValuePair getTimeValuePair() {
    return new TimeValuePair(timestamp, value);
  }

  @Override
  public int getSize() {
    return 1;
  }

  @Override
  public long getTimestamp(int index) {
    if (index == 0) {
      return timestamp;
    }
    throw new RuntimeException(INDEX_OPERATION_ON_MONAD_EXCEPTION);
  }

  @Override
  public void setTimestamp(int index, long timestamp) {
    if (index == 0) {
      this.timestamp = timestamp;
    }
    throw new RuntimeException(INDEX_OPERATION_ON_MONAD_EXCEPTION);
  }

  @Override
  public TsPrimitiveType getValue(int index) {
    if (index == 0) {
      return value;
    }
    throw new RuntimeException(INDEX_OPERATION_ON_MONAD_EXCEPTION);
  }

  @Override
  public void setValue(int index, TsPrimitiveType value) {
    if (index == 0) {
      this.value = value;
    }
    throw new RuntimeException(INDEX_OPERATION_ON_MONAD_EXCEPTION);
  }

  @Override
  public TimeValuePair getTimeValuePair(int index) {
    if (index != 0) {
      throw new RuntimeException(INDEX_OPERATION_ON_MONAD_EXCEPTION);
    } else if (value == null) {
      return null;
    } else {
      return new TimeValuePair(timestamp, value);
    }
  }
}
