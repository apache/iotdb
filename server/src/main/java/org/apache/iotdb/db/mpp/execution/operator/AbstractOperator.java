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

package org.apache.iotdb.db.mpp.execution.operator;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

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
  protected int startOffset;

  public void initializeMaxTsBlockLength(TsBlock tsBlock) {
    if (maxTupleSizeOfTsBlock != -1) {
      return;
    }
    long oneTupleSize = tsBlock.getRetainedSizeInBytes() / tsBlock.getPositionCount();
    this.maxTupleSizeOfTsBlock = (int) (maxReturnSize / oneTupleSize);
  }

  public TsBlock checkTsBlockSizeAndGetResult() {
    if (maxTupleSizeOfTsBlock == -1) {
      initializeMaxTsBlockLength(resultTsBlock);
    }

    if (resultTsBlock.getPositionCount() <= maxTupleSizeOfTsBlock) {
      return resultTsBlock;
    } else {
      retainedTsBlock = resultTsBlock;
      return getResultFromRetainedTsBlock();
    }
  }

  public TsBlock getResultFromRetainedTsBlock() {
    if (retainedTsBlock.getPositionCount() - startOffset <= maxTupleSizeOfTsBlock) {
      resultTsBlock = retainedTsBlock.subTsBlock(startOffset);
      retainedTsBlock = null;
      startOffset = 0;
    } else {
      resultTsBlock = retainedTsBlock.getRegion(startOffset, maxTupleSizeOfTsBlock);
      startOffset += maxTupleSizeOfTsBlock;
    }
    LOGGER.debug("Current tsBlock size is : {}", resultTsBlock.getRetainedSizeInBytes());
    return resultTsBlock;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }
}
