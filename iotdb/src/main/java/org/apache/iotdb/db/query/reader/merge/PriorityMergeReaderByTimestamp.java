/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.query.reader.merge;

import java.io.IOException;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;

/**
 * TODO the process of PriorityMergeReaderByTimestamp can be optimized.
 */
public class PriorityMergeReaderByTimestamp extends PriorityMergeReader implements
    EngineReaderByTimeStamp {

  private boolean hasCachedTimeValuePair;
  private TimeValuePair cachedTimeValuePair;

  @Override
  public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {

    if (hasCachedTimeValuePair) {
      if (cachedTimeValuePair.getTimestamp() == timestamp) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair.getValue();
      } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
        return null;
      }
    }

    while (hasNext()) {
      cachedTimeValuePair = next();
      if (cachedTimeValuePair.getTimestamp() == timestamp) {
        hasCachedTimeValuePair = false;
        return cachedTimeValuePair.getValue();
      } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
        hasCachedTimeValuePair = true;
        return null;
      }
    }

    return null;
  }
}
