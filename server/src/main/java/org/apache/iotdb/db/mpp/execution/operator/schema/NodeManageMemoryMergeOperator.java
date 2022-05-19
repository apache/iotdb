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

import org.apache.iotdb.confignode.rpc.thrift.NodeManagementType;
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
  private Set<String> data;
  private final Operator child;
  private NodeManagementType type;
  private boolean isFinished;

  public NodeManageMemoryMergeOperator(
      OperatorContext operatorContext, Set<String> data, Operator child, NodeManagementType type) {
    this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
    this.data = data;
    this.child = requireNonNull(child, "child operator is null");
    this.type = type;
    isFinished = false;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    return child.isBlocked();
  }

  @Override
  public TsBlock next() {
    isFinished = true;
    TsBlock block = child.next();
    TsBlockBuilder tsBlockBuilder;
    if (type == NodeManagementType.CHILD_PATHS) {
      tsBlockBuilder = new TsBlockBuilder(HeaderConstant.showChildPathsHeader.getRespDataTypes());
    } else {
      tsBlockBuilder = new TsBlockBuilder(HeaderConstant.showChildNodesHeader.getRespDataTypes());
    }
    Set<String> childPaths = new TreeSet<>(data);
    for (int i = 0; i < block.getPositionCount(); i++) {
      childPaths.add(block.getColumn(0).getBinary(i).toString());
    }

    childPaths.forEach(
        path -> {
          tsBlockBuilder.getTimeColumnBuilder().writeLong(0L);
          tsBlockBuilder.getColumnBuilder(0).writeBinary(new Binary(path));
          tsBlockBuilder.declarePosition();
        });
    return tsBlockBuilder.build();
  }

  @Override
  public boolean hasNext() {
    return child.hasNext();
  }

  @Override
  public void close() throws Exception {
    child.close();
  }

  @Override
  public boolean isFinished() {
    return isFinished || child.isFinished();
  }
}
