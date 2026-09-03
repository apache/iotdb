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

package org.apache.iotdb.db.pipe.event.common.tsfile.parser;

import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryBlock;

/** Allocates parser working memory from the pool owned by the caller. */
public interface TsFileInsertionEventParserMemoryManager {

  TsFileInsertionEventParserMemoryBlock forceAllocateForTabletWithRetry(
      String name, long sizeInBytes);

  TsFileInsertionEventParserMemoryBlock forceAllocate(String name, long sizeInBytes);

  static TsFileInsertionEventParserMemoryManager pipe() {
    return PipeHolder.INSTANCE;
  }

  final class PipeHolder {
    private static final TsFileInsertionEventParserMemoryManager INSTANCE =
        new TsFileInsertionEventParserMemoryManager() {
          @Override
          public TsFileInsertionEventParserMemoryBlock forceAllocateForTabletWithRetry(
              final String name, final long sizeInBytes) {
            return new PipeBlock(
                PipeDataNodeResourceManager.memory()
                    .forceAllocateForTabletWithRetry(name, sizeInBytes));
          }

          @Override
          public TsFileInsertionEventParserMemoryBlock forceAllocate(
              final String name, final long sizeInBytes) {
            return new PipeBlock(
                PipeDataNodeResourceManager.memory().forceAllocate(name, sizeInBytes));
          }
        };
  }

  final class PipeBlock implements TsFileInsertionEventParserMemoryBlock {
    private final PipeMemoryBlock delegate;

    private PipeBlock(final PipeMemoryBlock delegate) {
      this.delegate = delegate;
    }

    @Override
    public long getMemoryUsageInBytes() {
      return delegate.getMemoryUsageInBytes();
    }

    @Override
    public void forceResize(final long newSizeInBytes) {
      PipeDataNodeResourceManager.memory().forceResize(delegate, newSizeInBytes);
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
