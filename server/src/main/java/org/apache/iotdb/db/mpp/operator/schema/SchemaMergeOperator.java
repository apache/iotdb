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
package org.apache.iotdb.db.mpp.operator.schema;

import org.apache.iotdb.db.mpp.operator.Operator;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.util.List;

public class SchemaMergeOperator implements ProcessOperator {

  protected OperatorContext operatorContext;
  protected int limit;
  protected int offset;
  private final boolean[] noMoreTsBlocks;
  private boolean isFinished;

  private List<Operator> children;

  public SchemaMergeOperator(OperatorContext operatorContext, List<Operator> children) {
    this.operatorContext = operatorContext;
    this.children = children;
    noMoreTsBlocks = new boolean[children.size()];
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    // ToDo consider SHOW LATEST

    for (int i = 0; i < children.size(); i++) {
      if (!noMoreTsBlocks[i]) {
        TsBlock tsBlock = children.get(i).next();
        if (!children.get(i).hasNext()) {
          noMoreTsBlocks[i] = true;
        }
        return tsBlock;
      }
    }
    return null;
  }

  @Override
  public boolean hasNext() {
    for (int i = 0; i < children.size(); i++) {
      if (!noMoreTsBlocks[i] && children.get(i).hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
  }
}
