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

package org.apache.iotdb.db.pipe.resource.memory;

import org.apache.iotdb.db.pipe.resource.PipeResourceManager;

public class PipeMemoryBlock implements AutoCloseable {

  private final long memoryUsageInBytes;

  private volatile boolean isReleased = false;

  public PipeMemoryBlock(long memoryUsageInBytes) {
    this.memoryUsageInBytes = memoryUsageInBytes;
  }

  public long getMemoryUsageInBytes() {
    return memoryUsageInBytes;
  }

  boolean isReleased() {
    return isReleased;
  }

  void markAsReleased() {
    isReleased = true;
  }

  @Override
  public void close() {
    PipeResourceManager.memory().release(this);
  }
}
