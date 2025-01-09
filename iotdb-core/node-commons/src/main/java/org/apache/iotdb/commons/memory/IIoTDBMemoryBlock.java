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

package org.apache.iotdb.commons.memory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public abstract class IIoTDBMemoryBlock implements AutoCloseable{
  protected IoTDBMemoryManager memoryManager;
  protected IoTDBMemoryBlockType memoryBlockType;
  protected final ReentrantLock lock = new ReentrantLock();
  protected long maxMemorySizeInByte = 0;
  protected final AtomicLong memoryUsageInBytes = new AtomicLong(0);
  protected volatile boolean isReleased = false;

  public abstract boolean useMemory(final long size);

  public long getMaxMemorySizeInByte() {
    return maxMemorySizeInByte;
  }

  public long getMemoryUsageInBytes() {
    return memoryUsageInBytes.get();
  }

  public void setMaxMemorySizeInByte(final long maxMemorySizeInByte) {
    this.maxMemorySizeInByte = maxMemorySizeInByte;
  }

  public boolean isReleased() {
    return isReleased;
  }

  public void markAsReleased() {
    isReleased = true;
  }
}
