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

package org.apache.iotdb.commons.pipe.agent.task.progress;

import java.util.Objects;

public class CommitterKey {

  private final String pipeName;
  private final long creationTime;
  private final int regionId;
  private final int restartTimes;

  CommitterKey(final String pipeName, final long creationTime, final int regionId) {
    this(pipeName, creationTime, regionId, -1);
  }

  public CommitterKey(
      final String pipeName, final long creationTime, final int regionId, final int restartTimes) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
    this.regionId = regionId;
    this.restartTimes = restartTimes;
  }

  public String getPipeName() {
    return pipeName;
  }

  public long getCreationTime() {
    return creationTime;
  }

  public int getRegionId() {
    return regionId;
  }

  public int getRestartTimes() {
    return restartTimes;
  }

  public String stringify() {
    return String.format("%s_%s_%s_%s", pipeName, regionId, creationTime, restartTimes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeName, creationTime, regionId, restartTimes);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final CommitterKey that = (CommitterKey) obj;
    return Objects.equals(this.pipeName, that.pipeName)
        && Objects.equals(this.creationTime, that.creationTime)
        && Objects.equals(this.regionId, that.regionId)
        && Objects.equals(this.restartTimes, that.restartTimes);
  }

  @Override
  public String toString() {
    return "CommitterKey{pipeName="
        + pipeName
        + ", creationTime="
        + creationTime
        + ", regionId="
        + regionId
        + ", restartTimes="
        + restartTimes
        + "}";
  }
}
