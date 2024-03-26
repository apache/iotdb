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

package org.apache.iotdb.db.pipe.processor.aggregate.window.datastructure;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.Map;

public class WindowOutput {
  private long timestamp;
  private long progressTime;
  private Map<String, Pair<TSDataType, Object>> aggregatedResults;

  public long getTimestamp() {
    return timestamp;
  }

  public WindowOutput setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public long getProgressTime() {
    return progressTime;
  }

  public WindowOutput setProgressTime(long progressTime) {
    this.progressTime = progressTime;
    return this;
  }

  public Map<String, Pair<TSDataType, Object>> getAggregatedResults() {
    return aggregatedResults;
  }

  public WindowOutput setAggregatedResults(
      Map<String, Pair<TSDataType, Object>> aggregatedResults) {
    this.aggregatedResults = aggregatedResults;
    return this;
  }

  @Override
  public String toString() {
    return "WindowOutput{"
        + "timestamp='"
        + timestamp
        + "', progressTime='"
        + progressTime
        + "', aggregatedResults='"
        + aggregatedResults
        + "'}";
  }
}
