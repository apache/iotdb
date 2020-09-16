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
package org.apache.iotdb.db.timeIndex;

import org.apache.iotdb.db.metadata.PartialPath;

/**
 * Time Index, like deviceIndex: [(deviceId, startTime, endTime, TsFilePath)] to accelerate query
 */
public class TimeIndex {
  private PartialPath[] paths;
  private long[] startTimes;
  private long[] endTimes;
  private String tsFilePath;

  public PartialPath[] getPaths() {
    return paths;
  }

  public void setPaths(PartialPath[] paths) {
    this.paths = paths;
  }

  public long[] getStartTimes() {
    return startTimes;
  }

  public void setStartTimes(long[] startTimes) {
    this.startTimes = startTimes;
  }

  public long[] getEndTimes() {
    return endTimes;
  }

  public void setEndTimes(long[] endTimes) {
    this.endTimes = endTimes;
  }

  public String getTsFilePath() {
    return tsFilePath;
  }

  public void setTsFilePath(String tsFilePath) {
    this.tsFilePath = tsFilePath;
  }
}