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

package org.apache.iotdb.db.storageengine.load.memory;

import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParserMemoryBlock;
import org.apache.iotdb.db.pipe.event.common.tsfile.parser.TsFileInsertionEventParserMemoryManager;

/**
 * Allocates the working memory of TsFile parsers reused by Load from the query engine memory pool.
 */
public class LoadTsFileParserMemoryManager implements TsFileInsertionEventParserMemoryManager {

  private static final LoadTsFileMemoryManager LOAD_MEMORY_MANAGER =
      LoadTsFileMemoryManager.getInstance();

  private LoadTsFileParserMemoryManager() {}

  public static LoadTsFileParserMemoryManager getInstance() {
    return LoadTsFileParserMemoryManagerHolder.INSTANCE;
  }

  @Override
  public TsFileInsertionEventParserMemoryBlock forceAllocateForTabletWithRetry(
      final long sizeInBytes) {
    return new LoadParserMemoryBlock(sizeInBytes);
  }

  @Override
  public TsFileInsertionEventParserMemoryBlock forceAllocate(final long sizeInBytes) {
    return new LoadParserMemoryBlock(sizeInBytes);
  }

  private static class LoadParserMemoryBlock implements TsFileInsertionEventParserMemoryBlock {

    private LoadTsFileMemoryBlock delegate;
    private long memoryUsageInBytes;
    private boolean isClosed;

    private LoadParserMemoryBlock(final long sizeInBytes) {
      checkNonNegative(sizeInBytes);
      if (sizeInBytes > 0) {
        delegate = LOAD_MEMORY_MANAGER.allocateMemoryBlock(sizeInBytes);
      }
      memoryUsageInBytes = sizeInBytes;
    }

    @Override
    public synchronized long getMemoryUsageInBytes() {
      return memoryUsageInBytes;
    }

    @Override
    public synchronized void forceResize(final long newSizeInBytes) {
      checkNonNegative(newSizeInBytes);
      if (isClosed || memoryUsageInBytes == newSizeInBytes) {
        return;
      }

      resizeDelegate(newSizeInBytes);
      memoryUsageInBytes = newSizeInBytes;
    }

    private void resizeDelegate(final long newSizeInBytes) {
      if (newSizeInBytes == 0) {
        delegate.close();
        delegate = null;
      } else if (delegate == null) {
        delegate = LOAD_MEMORY_MANAGER.allocateMemoryBlock(newSizeInBytes);
      } else {
        delegate.forceResize(newSizeInBytes);
      }
    }

    @Override
    public synchronized void close() {
      if (isClosed) {
        return;
      }
      isClosed = true;
      memoryUsageInBytes = 0;
      if (delegate != null) {
        delegate.close();
        delegate = null;
      }
    }

    private static void checkNonNegative(final long sizeInBytes) {
      if (sizeInBytes < 0) {
        throw new IllegalArgumentException(
            String.format("Load: Invalid memory size %d bytes, must be non-negative", sizeInBytes));
      }
    }
  }

  private static class LoadTsFileParserMemoryManagerHolder {
    private static final LoadTsFileParserMemoryManager INSTANCE =
        new LoadTsFileParserMemoryManager();
  }
}
