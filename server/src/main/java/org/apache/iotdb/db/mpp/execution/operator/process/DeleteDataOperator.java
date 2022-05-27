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

package org.apache.iotdb.db.mpp.execution.operator.process;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.metadata.cache.DataNodeSchemaBlacklist;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.execution.driver.DataDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class DeleteDataOperator implements SourceOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteDataOperator.class);

  private final PlanNodeId planNodeId;
  private final OperatorContext operatorContext;
  private final List<PartialPath> pathList;
  private final String storageGroup;

  private boolean isFinished = false;

  public DeleteDataOperator(
      PlanNodeId planNodeId,
      OperatorContext operatorContext,
      List<PartialPath> pathList,
      String storageGroup) {
    this.planNodeId = planNodeId;
    this.operatorContext = operatorContext;
    this.pathList = pathList;
    this.storageGroup = storageGroup;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    isFinished = true;
    try {
      executeDeleteData();
      return new TsBlock(
          new TimeColumn(1, new long[] {0}),
          new BooleanColumn(1, Optional.of(new boolean[] {false}), new boolean[] {true}));
    } catch (IOException e) {
      LOGGER.error("Error occurred when deleting data. {}", planNodeId, e);
      return new TsBlock(
          new TimeColumn(1, new long[] {0}),
          new BooleanColumn(1, Optional.of(new boolean[] {false}), new boolean[] {true}),
          new BinaryColumn(
              1,
              Optional.of(new boolean[] {false}),
              new Binary[] {new Binary(e.getMessage().getBytes())}));
    }
  }

  @Override
  public boolean hasNext() {
    return !isFinished;
  }

  @Override
  public boolean isFinished() {
    return isFinished;
  }

  @Override
  public PlanNodeId getSourceId() {
    return planNodeId;
  }

  private void executeDeleteData() throws IOException {
    constructSchemaBlacklist();
    DataRegion dataRegion =
        ((DataDriverContext) operatorContext.getInstanceContext().getDriverContext())
            .getDataRegion();
    for (PartialPath path : pathList) {
      dataRegion.delete(path, Long.MIN_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, null);
    }
  }

  private void constructSchemaBlacklist() {
    PathPatternTree patternTree = new PathPatternTree();
    for (PartialPath path : pathList) {
      try {
        patternTree.appendPaths(path.alterPrefixPath(new PartialPath(storageGroup)));
      } catch (IllegalPathException e) {
        // this definitely won't happen
        throw new RuntimeException(e);
      }
    }
    DataNodeSchemaBlacklist.getInstance().appendToBlacklist(patternTree);
  }
}
