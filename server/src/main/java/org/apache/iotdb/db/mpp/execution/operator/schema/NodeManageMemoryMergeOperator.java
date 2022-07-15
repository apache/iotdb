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

package org.apache.iotdb.db.mpp.execution.operator.schema;

import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Set;
import java.util.TreeSet;

import static java.util.Objects.requireNonNull;

public class NodeManageMemoryMergeOperator implements ProcessOperator {
  private final OperatorContext operatorContext;
  private final Set<String> data;
  private final Operator child;
  private boolean isReadingMemory;

  public NodeManageMemoryMergeOperator(
      OperatorContext operatorContext, Set<String> data, Operator child) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    this.data = data;
    this.child = requireNonNull(child, "child operator is null");
    isReadingMemory = true;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return isReadingMemory ? NOT_BLOCKED : child.isBlocked();
  }

  @Override
  public TsBlock next() {
    if (isReadingMemory) {
      isReadingMemory = false;
      return transferToTsBlock(data);
    } else {
      TsBlock block = child.next();
      if (block == null) {
        return null;
      }

      Set<String> nodePaths = new TreeSet<>();
      String nodePath;
      for (int i = 0; i < block.getPositionCount(); i++) {
        nodePath = block.getColumn(0).getBinary(i).toString();
        if (!data.contains(nodePath)) {
          nodePaths.add(nodePath);
          data.add(nodePath);
        }
      }
      return transferToTsBlock(nodePaths);
    }
  }

  private TsBlock transferToTsBlock(Set<String> nodePaths) {
    TsBlockBuilder tsBlockBuilder =
        new TsBlockBuilder(HeaderConstant.showChildPathsHeader.getRespDataTypes());

    nodePaths.forEach(
        path -> {
          tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
          tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(path));
          tsBlockBuilder.declarePosition();
        });
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    return isReadingMemory || child.hasNext();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() {
    return !isReadingMemory && child.isFinished();
  }
}
