/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOperator implements Operator {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOperator.class);
  protected OperatorContext operatorContext;

  protected long maxReturnSize =
      TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
  protected int maxTupleSizeOfTsBlock = -1;
  protected TsBlock resultTsBlock;
  protected TsBlock retainedTsBlock;
  protected int startOffset = 0;

  public void initializeMaxTsBlockLength(TsBlock tsBlock) {
    if (maxTupleSizeOfTsBlock != -1) {
      return;
    }
    // oneTupleSize should be greater than 0 to avoid division by zero
    long oneTupleSize =
        Math.max(
            1,
            (tsBlock.getRetainedSizeInBytes() - tsBlock.getTotalInstanceSize())
                / tsBlock.getPositionCount());
    if (oneTupleSize > maxReturnSize) {
      // make sure at least one-tuple-at-a-time
      this.maxTupleSizeOfTsBlock = 1;
      LOGGER.warn("Only one tuple can be sent each time caused by limited memory");
    } else {
      this.maxTupleSizeOfTsBlock = (int) (maxReturnSize / oneTupleSize);
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("maxTupleSizeOfTsBlock is：{}", maxTupleSizeOfTsBlock);
    }
  }

  public TsBlock checkTsBlockSizeAndGetResult() {
    if (resultTsBlock == null) {
      throw new IllegalArgumentException("Result tsBlock cannot be null");
    } else if (resultTsBlock.isEmpty()) {
      TsBlock res = resultTsBlock;
      resultTsBlock = null;
      return res;
    }
    if (maxTupleSizeOfTsBlock == -1) {
      initializeMaxTsBlockLength(resultTsBlock);
    }

    if (resultTsBlock.getPositionCount() <= maxTupleSizeOfTsBlock) {
      TsBlock res = resultTsBlock;
      resultTsBlock = null;
      return res;
    } else {
      retainedTsBlock = resultTsBlock;
      resultTsBlock = null;
      return getResultFromRetainedTsBlock();
    }
  }

  public TsBlock getResultFromRetainedTsBlock() {
    TsBlock res;
    if (maxTupleSizeOfTsBlock == -1) {
      initializeMaxTsBlockLength(retainedTsBlock);
    }
    if (retainedTsBlock.getPositionCount() - startOffset <= maxTupleSizeOfTsBlock) {
      res = retainedTsBlock.subTsBlock(startOffset);
      retainedTsBlock = null;
      startOffset = 0;
    } else {
      res = retainedTsBlock.getRegion(startOffset, maxTupleSizeOfTsBlock);
      startOffset += maxTupleSizeOfTsBlock;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Current tsBlock size is : {}", res.getRetainedSizeInBytes());
    }
    return res;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }
}
