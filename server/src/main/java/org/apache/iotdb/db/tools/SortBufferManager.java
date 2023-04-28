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

  public SortBufferManager() {}

  public long SORT_BUFFER_SIZE = IoTDBDescriptor.getInstance().getConfig().getSortBufferSize();
  private long bufferUsed = 0;

  private final long BUFFER_SIZE_FOR_ONE_BRANCH = DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

  public synchronized void allocateOneSortBranch() {
    boolean checked = check(BUFFER_SIZE_FOR_ONE_BRANCH);
    if (!checked) throw new IllegalArgumentException("Not enough memory for sorting");
    bufferUsed += BUFFER_SIZE_FOR_ONE_BRANCH;
  }

  public synchronized boolean check(long size) {
    return bufferUsed + size < SORT_BUFFER_SIZE;
  }

  public synchronized boolean allocate(long size) {
    if (check(size)) {
      bufferUsed += size;
      return true;
    }
    return false;
  }

  public synchronized void releaseOneSortBranch() {
    bufferUsed -= BUFFER_SIZE_FOR_ONE_BRANCH;
  }

  public synchronized void setSortBufferSize(long size) {
    SORT_BUFFER_SIZE = size;
  }

  public synchronized long getBufferAvailable() {
    return SORT_BUFFER_SIZE - bufferUsed;
  }
}
