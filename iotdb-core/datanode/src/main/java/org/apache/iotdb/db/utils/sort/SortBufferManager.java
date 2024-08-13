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

package org.apache.iotdb.db.utils.sort;

public class SortBufferManager {

  private final int maxTsBlockSizeInBytes;
  private final long sortBufferSize;

  private long bufferUsed;

  private final long bufferSizeForOneBranch;

  private final long bufferAvailableForAllBranch;
  private long readerBuffer = 0;
  private long branchNum = 0;

  public SortBufferManager(int maxTsBlockSizeInBytes, long sortBufferSize) {
    this.maxTsBlockSizeInBytes = maxTsBlockSizeInBytes;
    this.sortBufferSize = sortBufferSize;
    this.bufferAvailableForAllBranch = sortBufferSize - maxTsBlockSizeInBytes;
    this.bufferSizeForOneBranch = maxTsBlockSizeInBytes;
    // the initial value is the buffer for output.
    this.bufferUsed = maxTsBlockSizeInBytes;
  }

  public void allocateOneSortBranch() {
    boolean success = allocate(bufferSizeForOneBranch);
    if (!success) {
      throw new IllegalArgumentException("Not enough memory for sorting");
    }
    branchNum++;
  }

  private boolean check(long size) {
    return bufferUsed + size < sortBufferSize;
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
    if (branchNum != 0) {
      readerBuffer = bufferAvailableForAllBranch / branchNum;
    }
  }

  public long getReaderBufferAvailable() {
    if (readerBuffer != 0) {
      return readerBuffer;
    }
    readerBuffer = bufferAvailableForAllBranch / branchNum;
    return readerBuffer;
  }

  public int getMaxTsBlockSizeInBytes() {
    return maxTsBlockSizeInBytes;
  }

  public long getSortBufferSize() {
    return sortBufferSize;
  }
}
