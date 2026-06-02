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

import org.apache.iotdb.db.i18n.DataNodePipeMessages;

import java.util.function.BiConsumer;
import java.util.function.LongUnaryOperator;

public abstract class PipeFixedMemoryBlock extends PipeMemoryBlock {

  public PipeFixedMemoryBlock(long memoryUsageInBytes) {
    super(memoryUsageInBytes);
  }

  @Override
  boolean shrink() {
    return false;
  }

  @Override
  boolean expand() {
    return false;
  }

  @Override
  public PipeMemoryBlock setShrinkMethod(LongUnaryOperator shrinkMethod) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.SHRINK_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK);
  }

  @Override
  public PipeMemoryBlock setShrinkCallback(BiConsumer<Long, Long> shrinkCallback) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.SHRINK_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK);
  }

  @Override
  public PipeMemoryBlock setExpandMethod(LongUnaryOperator extendMethod) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.EXPAND_METHOD_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK);
  }

  @Override
  public PipeMemoryBlock setExpandCallback(BiConsumer<Long, Long> expandCallback) {
    throw new UnsupportedOperationException(
        DataNodePipeMessages.EXPAND_CALLBACK_IS_NOT_SUPPORTED_IN_PIPEFIXEDMEMORYBLOCK);
  }
}
