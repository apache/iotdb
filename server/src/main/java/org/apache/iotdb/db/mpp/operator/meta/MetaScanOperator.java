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
package org.apache.iotdb.db.mpp.operator.meta;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.operator.source.SourceOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.util.List;

public abstract class MetaScanOperator implements SourceOperator {

  protected OperatorContext operatorContext;
  protected TsBlock tsBlock;
  private boolean hasCachedTsBlock;

  protected int limit;
  protected int offset;
  protected PartialPath partialPath;
  protected boolean isPrefixPath;
  protected List<String> columns;

  protected MetaScanOperator(
      OperatorContext operatorContext,
      int limit,
      int offset,
      PartialPath partialPath,
      boolean isPrefixPath,
      List<String> columns) {
    this.operatorContext = operatorContext;
    this.limit = limit;
    this.offset = offset;
    this.partialPath = partialPath;
    this.isPrefixPath = isPrefixPath;
    this.columns = columns;
  }

  protected abstract TsBlock createTsBlock();

  public PartialPath getPartialPath() {
    return partialPath;
  }

  public int getLimit() {
    return limit;
  }

  public int getOffset() {
    return offset;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  public boolean isPrefixPath() {
    return isPrefixPath;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    hasCachedTsBlock = false;
    return tsBlock;
  }

  @Override
  public boolean hasNext() {
    if (tsBlock == null) {
      tsBlock = createTsBlock();
      if (tsBlock.getPositionCount() > 0) {
        hasCachedTsBlock = true;
      }
    }
    return hasCachedTsBlock;
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
  }
}
