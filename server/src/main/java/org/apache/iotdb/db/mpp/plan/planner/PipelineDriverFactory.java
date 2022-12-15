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
import org.apache.iotdb.db.mpp.execution.driver.Driver;
import org.apache.iotdb.db.mpp.execution.driver.DriverContext;
import org.apache.iotdb.db.mpp.execution.operator.Operator;
import org.apache.iotdb.db.mpp.execution.operator.OperatorContext;

import javax.annotation.concurrent.GuardedBy;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class PipelineDriverFactory {

  private final int pipelineId;
  private final DriverContext driverContext;
  private final boolean inputDriver;
  private final boolean outputDriver;
  private final Operator rootOperator;
  // private final List<PlanNodeId> sourceId;
  // 控制 Instance 的数量 = 并行度
  // private final OptionalInt driverInstances;

  @GuardedBy("this")
  private boolean noMoreDrivers;

  public PipelineDriverFactory(
      int pipelineId,
      boolean inputDriver,
      boolean outputDriver,
      Operator operator,
      DriverContext driverContext) {
    this.pipelineId = pipelineId;
    this.inputDriver = inputDriver;
    this.outputDriver = outputDriver;
    this.rootOperator = requireNonNull(operator, "rootOperator is null");
    // this.driverInstances = requireNonNull(driverInstances, "driverInstances is null");
    this.driverContext = driverContext;
    //    this.sourceId =
    //        ((DataDriverContext) driverContext)
    //            .getSourceOperators().stream()
    //                .map(SourceOperator::getSourceId)
    //                .collect(Collectors.toList());
  }

  public DriverContext getDriverContext() {
    return driverContext;
  }

  public int getPipelineId() {
    return pipelineId;
  }

  public boolean isInputDriver() {
    return inputDriver;
  }

  public boolean isOutputDriver() {
    return outputDriver;
  }

  //    /**
  //     * return the sourceId of this DriverFactory.
  //     * A DriverFactory doesn't always have source node.
  //     * For example, ValuesNode is not a source node.
  //     */
  //    public Optional<PlanNodeId> getSourceId()
  //    {
  //        return sourceId;
  //    }

  //    public OptionalInt getDriverInstances()
  //    {
  //        return driverInstances;
  //    }

  // thinking: why synchronized
  public synchronized Driver createDriver() {
    checkState(!noMoreDrivers, "noMoreDrivers is already set");
    requireNonNull(driverContext, "driverContext is null");
    try {
      return new DataDriver(rootOperator, driverContext);
    } catch (Throwable failure) {

      try {
        rootOperator.close();
      } catch (Throwable closeFailure) {
        if (failure != closeFailure) {
          failure.addSuppressed(closeFailure);
        }
      }
      for (OperatorContext operatorContext : driverContext.getOperatorContexts()) {
        try {
          // operatorContext.destroy();
        } catch (Throwable destroyFailure) {
          if (failure != destroyFailure) {
            failure.addSuppressed(destroyFailure);
          }
        }
      }
      throw failure;
    }
  }

  //    public synchronized void noMoreDrivers()
  //    {
  //        if (noMoreDrivers) {
  //            return;
  //        }
  //        noMoreDrivers = true;
  //        for (OperatorFactory operatorFactory : operatorFactories) {
  //            operatorFactory.noMoreOperators();
  //        }
  //    }

  public synchronized boolean isNoMoreDrivers() {
    return noMoreDrivers;
  }
}
