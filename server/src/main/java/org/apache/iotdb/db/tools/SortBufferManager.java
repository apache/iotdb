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

package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class SortBufferManager {

  public static final long SORT_BUFFER_SIZE =
      IoTDBDescriptor.getInstance().getConfig().getSortBufferSize();

  private long bufferUsed;

  private final long BUFFER_SIZE_FOR_ONE_BRANCH = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

  private final long BUFFER_AVAILABLE_FOR_ALL_BRANCH;
  private long readerBuffer = 0;
  private long branchNum = 0;

  public SortBufferManager() {
    this.BUFFER_AVAILABLE_FOR_ALL_BRANCH = SORT_BUFFER_SIZE - DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
    // the initial value is the buffer for output.
    this.bufferUsed = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  public void allocateOneSortBranch() {
    boolean success = allocate(BUFFER_SIZE_FOR_ONE_BRANCH);
    if (!success) throw new IllegalArgumentException("Not enough memory for sorting");
    branchNum++;
  }

  private boolean check(long size) {
    return bufferUsed + size < SORT_BUFFER_SIZE;
  }

  public boolean allocate(long size) {
    if (check(size)) {
      bufferUsed += size;
      return true;
    }
    return false;
  }

  public void releaseOneSortBranch() {
    branchNum--;
    if (branchNum != 0) readerBuffer = BUFFER_AVAILABLE_FOR_ALL_BRANCH / branchNum;
  }

  public long getReaderBufferAvailable() {
    if (readerBuffer != 0) return readerBuffer;
    readerBuffer = BUFFER_AVAILABLE_FOR_ALL_BRANCH / branchNum;
    return readerBuffer;
  }
}
