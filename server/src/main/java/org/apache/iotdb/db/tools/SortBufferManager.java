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

import static org.apache.iotdb.tsfile.read.common.block.TsBlockBuilderStatus.DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;

public class SortBufferManager {

  public static long SORT_BUFFER_SIZE = 50 * 1024 * 1024L;
  private static long bufferUsed = 0;

  public static synchronized void allocateOneSortBranch() {
    boolean checked = check(DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES);
    if (!checked) throw new IllegalArgumentException("No enough memory for sort");
    bufferUsed += DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  public static synchronized boolean check(long size) {
    return bufferUsed + size < SORT_BUFFER_SIZE;
  }

  public static synchronized boolean allocate(long size) {
    if (check(size)) {
      bufferUsed += size;
      return true;
    }
    return false;
  }

  public static synchronized void releaseOneSortBranch() {
    bufferUsed -= DEFAULT_MAX_TSBLOCK_SIZE_IN_BYTES;
  }

  public synchronized void releaseAllSortBranch() {
    bufferUsed = 0;
  }

  public synchronized void setSortBufferSize(long size) {
    SORT_BUFFER_SIZE = size;
  }

  public synchronized long getSortBufferSize() {
    return SORT_BUFFER_SIZE;
  }

  public synchronized long getBufferUsed() {
    return bufferUsed;
  }

  public static synchronized long getBufferAvailable() {
    return SORT_BUFFER_SIZE - bufferUsed;
  }
}
