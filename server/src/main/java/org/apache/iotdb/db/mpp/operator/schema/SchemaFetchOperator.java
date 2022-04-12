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

package org.apache.iotdb.db.mpp.operator.schema;

import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.operator.source.SourceOperator;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import java.io.IOException;
import java.util.NoSuchElementException;

public class SchemaFetchOperator implements SourceOperator {

  private final PlanNodeId sourceId;
  private final OperatorContext operatorContext;
  private final PathPatternTree patternTree;

  private TsBlock tsBlock;
  private boolean isFinished = false;

  public SchemaFetchOperator(
      PlanNodeId planNodeId, OperatorContext context, PathPatternTree patternTree) {
    this.sourceId = planNodeId;
    this.operatorContext = context;
    this.patternTree = patternTree;
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
    return tsBlock;
  }

  @Override
  public boolean hasNext() {
    return isFinished;
  }

  @Override
  public boolean isFinished() throws IOException {
    return isFinished;
  }

  @Override
  public PlanNodeId getSourceId() {
    return sourceId;
  }

  private void fetchSchema() {
    // todo
  }
}
