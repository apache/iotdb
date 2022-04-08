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
package org.apache.iotdb.db.mpp.operator.source;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.mpp.operator.OperatorContext;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;

import com.google.common.util.concurrent.ListenableFuture;

public class SeriesAggregateScanOperator implements SourceOperator {
  @Override
  public OperatorContext getOperatorContext() {
    return null;
  }

  @Override
  public ListenableFuture<Void> isBlocked() {
    return SourceOperator.super.isBlocked();
  }

  @Override
  public TsBlock next() {
    return null;
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public void close() throws Exception {
    SourceOperator.super.close();
  }

  @Override
  public boolean isFinished() {
    return false;
  }

  @Override
  public PlanNodeId getSourceId() {
    return null;
  }

  @Override
  public void initQueryDataSource(QueryDataSource dataSource) {}
}
