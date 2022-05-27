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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;
import org.apache.iotdb.db.mpp.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.mpp.execution.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.BinaryColumn;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class DeleteTimeseriesOperator implements ProcessOperator, SourceOperator {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTimeseriesOperator.class);

  private final PlanNodeId planNodeId;
  private final OperatorContext operatorContext;
  private final List<Operator> children;

  private final boolean[] childrenFinishStatus;

  private final List<PartialPath> pathList;
  private boolean isFinished = false;

  public DeleteTimeseriesOperator(
      PlanNodeId planNodeId,
      OperatorContext operatorContext,
      List<PartialPath> pathList,
      List<Operator> children) {
    this.planNodeId = planNodeId;
    this.operatorContext = operatorContext;
    this.children = children;
    this.childrenFinishStatus = new boolean[children.size()];
    this.pathList = pathList;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return null;
  }

  @Override
  public TsBlock next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    for (int i = 0; i < children.size(); i++) {
      if (!childrenFinishStatus[i]) {
        TsBlock tsBlock = children.get(i).next();
        if (!children.get(i).hasNext()) {
          childrenFinishStatus[i] = true;
        }
        return tsBlock;
      }
    }

    isFinished = true;
    try {
      executeDeleteTimeseries();
      return new TsBlock(
          new TimeColumn(1, new long[] {0}),
          new BooleanColumn(1, Optional.of(new boolean[] {false}), new boolean[] {true}));
    } catch (MetadataException e) {
      LOGGER.error("Error occurred when deleting timeseries. {}", planNodeId, e);
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
    for (int i = 0; i < children.size(); i++) {
      if (!childrenFinishStatus[i] && children.get(i).hasNext()) {
        return true;
      }
    }
    return !isFinished;
  }

  @Override
  public boolean isFinished() {
    return !hasNext();
  }

  @Override
  public PlanNodeId getSourceId() {
    return planNodeId;
  }

  private void executeDeleteTimeseries() throws MetadataException {
    ISchemaRegion schemaRegion =
        ((SchemaDriverContext) operatorContext.getInstanceContext().getDriverContext())
            .getSchemaRegion();
    for (PartialPath path : pathList) {
      schemaRegion.deleteTimeseries(path, false);
    }
  }
}
