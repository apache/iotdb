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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.region;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ExtendRegion;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.region.ExtendRegionStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class ExtendRegionTask implements IConfigTask {

  protected final ExtendRegionStatement statement;
  private final Model model;

  public ExtendRegionTask(ExtendRegionStatement statement) {
    this.statement = statement;
    this.model = Model.TREE;
  }

  public ExtendRegionTask(ExtendRegion extendRegion) {
    this.statement =
        new ExtendRegionStatement(extendRegion.getRegionIds(), extendRegion.getDataNodeId());
    this.model = Model.TABLE;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.extendRegion(this);
  }

  public ExtendRegionStatement getStatement() {
    return statement;
  }

  public Model getModel() {
    return model;
  }
}
