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

package org.apache.iotdb.db.metadata.cache.lastCache.container.value;

import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

public class NullLastCacheValue implements ILastCacheValue {
  @Override
  public long getTimestamp() {
    throw new UnsupportedOperationException("NullLastCacheValue do not support getTimeStamp()");
  }

  @Override
  public void setTimestamp(long timestamp) {
    throw new UnsupportedOperationException("NullLastCacheValue do not support setTimeStamp()");
  }

  @Override
  public void setValue(TsPrimitiveType value) {
    throw new UnsupportedOperationException("NullLastCacheValue do not support setValue()");
  }

  @Override
  public TimeValuePair getTimeValuePair() {
    throw new UnsupportedOperationException("NullLastCacheValue do not support getTimeValuePair()");
  }

  @Override
  public boolean isNull() {
    return true;
  }
}
