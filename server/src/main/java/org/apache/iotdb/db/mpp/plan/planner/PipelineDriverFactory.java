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

package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.db.mpp.execution.driver.DataDriver;
import org.apache.iotdb.db.mpp.execution.driver.DataDriverContext;
import org.apache.iotdb.db.mpp.execution.driver.Driver;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriver;
import org.apache.iotdb.db.mpp.execution.driver.SchemaDriverContext;
import org.apache.iotdb.db.mpp.execution.operator.Operator;

import static java.util.Objects.requireNonNull;

public class PipelineDriverFactory {

  private final DriverContext driverContext;
  // TODO Use OperatorFactory to replace operator to generate multiple drivers for on pipeline
  private final Operator operation;
  private int dependencyPipelineIndex = -1;

  public PipelineDriverFactory(Operator operation, DriverContext driverContext) {
    this.operation = requireNonNull(operation, "rootOperator is null");
    this.driverContext = driverContext;
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  public Driver createDriver() {
    requireNonNull(driverContext, "driverContext is null");
    try {
      Driver driver = null;
      if (driverContext instanceof DataDriverContext) {
        driver = new DataDriver(operation, driverContext);
      } else {
        driver = new SchemaDriver(operation, (SchemaDriverContext) driverContext);
      }
      if (dependencyPipelineIndex != -1) {
        driver.getDriverContext().setDependencyDriverIndex(dependencyPipelineIndex);
      }
      return driver;
    } catch (Throwable failure) {
      try {
        operation.close();
      } catch (Throwable closeFailure) {
        if (failure != closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
      throw failure;
    }
  }

  public void setDependencyPipeline(int dependencyPipelineIndex) {
    this.dependencyPipelineIndex = dependencyPipelineIndex;
  }

  public int getDependencyPipelineIndex() {
    return dependencyPipelineIndex;
  }
}
