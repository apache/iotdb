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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateFunction;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateFunctionStatement;
import org.apache.iotdb.udf.api.UDF;
import org.apache.iotdb.udf.api.relational.SQLFunction;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Optional;

public class CreateFunctionTask implements IConfigTask {

  private final Model model;
  private final String udfName;
  private final String className;
  private final Optional<String> uriString;
  private final Class<?> baseClazz;

  public CreateFunctionTask(CreateFunctionStatement createFunctionStatement) {
    this.udfName = createFunctionStatement.getUdfName();
    this.className = createFunctionStatement.getClassName();
    this.uriString = createFunctionStatement.getUriString();
    this.baseClazz = UDF.class; // Tree Model
    this.model = Model.TREE;
  }

  public CreateFunctionTask(CreateFunction createFunctionStatement) {
    this.udfName = createFunctionStatement.getUdfName();
    this.className = createFunctionStatement.getClassName();
    this.uriString = createFunctionStatement.getUriString();
    this.baseClazz = SQLFunction.class; // Table Model
    this.model = Model.TABLE;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.createFunction(model, udfName, className, uriString, baseClazz);
  }
}
