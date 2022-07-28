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
package org.apache.iotdb.tsfile.read.common.block;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilderStatus;

public class TsBlockBuilderStatus {

  public static final int DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

  private final int maxTsBlockSizeInBytes;

  private long currentSize;

  public TsBlockBuilderStatus() {
    this(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
  }

  public TsBlockBuilderStatus(int maxTsBlockSizeInBytes) {
    this.maxTsBlockSizeInBytes = maxTsBlockSizeInBytes;
  }

  public ColumnBuilderStatus createColumnBuilderStatus() {
    return new ColumnBuilderStatus(this);
  }

  public int getMaxTsBlockSizeInBytes() {
    return maxTsBlockSizeInBytes;
  }

  public boolean isEmpty() {
    return currentSize == 0;
  }

  public boolean isFull() {
    return currentSize >= maxTsBlockSizeInBytes;
  }

  public void addBytes(int bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("bytes cannot be negative");
    }
    currentSize += bytes;
  }

  public long getSizeInBytes() {
    return currentSize;
  }

  @Override
  public String toString() {
    return "TsBlockBuilderStatus{"
        + "maxTsBlockSizeInBytes="
        + maxTsBlockSizeInBytes
        + ", currentSize="
        + currentSize
        + '}';
  }
}
