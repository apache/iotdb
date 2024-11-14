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

package org.apache.iotdb.commons.pipe.agent.task.meta;

import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTemporaryMetaInAgent implements PipeTemporaryMeta {

  // Statistics
  private final AtomicLong memoryUsage = new AtomicLong(0L);

  // Object pool
  private final String pipeNameWithCreationTime;
  private final Map<Integer, CommitterKey> regionId2CommitterKeyMap = new HashMap<>();

  PipeTemporaryMetaInAgent(final String pipeName, final long creationTime) {
    this.pipeNameWithCreationTime = pipeName + "_" + creationTime;
  }

  /////////////////////////////// DataNode ///////////////////////////////

  public void addMemoryUsage(final long usage) {
    memoryUsage.addAndGet(usage);
  }

  public void decreaseMemoryUsage(final long usage) {
    memoryUsage.addAndGet(-usage);
  }

  public String getPipeNameWithCreationTime() {
    return pipeNameWithCreationTime;
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PipeTemporaryMetaInAgent that = (PipeTemporaryMetaInAgent) o;
    return Objects.equals(this.memoryUsage.get(), that.memoryUsage.get());
  }

  @Override
  public int hashCode() {
    return Objects.hash(memoryUsage);
  }

  @Override
  public String toString() {
    return "PipeTemporaryMeta{" + "memoryUsage=" + memoryUsage + '}';
  }
}
