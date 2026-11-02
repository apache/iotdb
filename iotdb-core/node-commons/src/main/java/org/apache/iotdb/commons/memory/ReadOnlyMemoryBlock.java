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

import org.apache.iotdb.commons.i18n.CommonMessages;

import java.util.function.LongSupplier;

/**
 * A budgeted view of memory owned and counted by another component. The supplier must provide its
 * own synchronization; allocation and release remain the owner's responsibility.
 */
public final class ReadOnlyMemoryBlock extends IMemoryBlock {
  private final LongSupplier usedMemory;

  ReadOnlyMemoryBlock(
      String name,
      MemoryManager manager,
      long totalMemorySizeInBytes,
      MemoryBlockType type,
      LongSupplier usedMemory) {
    this.name = name;
    this.memoryManager = manager;
    this.totalMemorySizeInBytes = totalMemorySizeInBytes;
    this.memoryBlockType = type;
    this.usedMemory = usedMemory;
  }

  @Override
  public long getUsedMemoryInBytes() {
    return usedMemory.getAsLong();
  }

  @Override
  public long getFreeMemoryInBytes() {
    return totalMemorySizeInBytes - getUsedMemoryInBytes();
  }

  @Override
  public long forceAllocateWithoutLimitation(long sizeInByte) {
    throw readOnly();
  }

  @Override
  public boolean allocate(long sizeInByte) {
    throw readOnly();
  }

  @Override
  public boolean allocateIfSufficient(long sizeInByte, double maxRatio) {
    throw readOnly();
  }

  @Override
  public boolean allocateUntilAvailable(long sizeInByte, long retryIntervalInMillis) {
    throw readOnly();
  }

  @Override
  public long release(long sizeInByte) {
    throw readOnly();
  }

  @Override
  public void setUsedMemoryInBytes(long usedMemoryInBytes) {
    throw readOnly();
  }

  private UnsupportedOperationException readOnly() {
    return new UnsupportedOperationException(
        String.format(CommonMessages.EXCEPTION_MEMORY_BLOCK_ARG_IS_READ_ONLY_D10B3340, name));
  }

  @Override
  public void close() {
    memoryManager.release(this);
  }

  @Override
  public String toString() {
    return "ReadOnlyMemoryBlock{name="
        + name
        + ", isReleased="
        + isReleased
        + ", memoryBlockType="
        + memoryBlockType
        + ", totalMemorySizeInBytes="
        + totalMemorySizeInBytes
        + ", usedMemoryInBytes="
        + getUsedMemoryInBytes()
        + '}';
  }
}
