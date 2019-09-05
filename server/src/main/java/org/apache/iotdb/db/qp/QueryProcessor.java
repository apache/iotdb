/**
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
package org.apache.iotdb.db.qp;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.qp.LogicalOperatorException;
import org.apache.iotdb.db.exception.qp.LogicalOptimizeException;
import org.apache.iotdb.db.exception.qp.QueryProcessorException;
import org.apache.iotdb.db.qp.executor.IQueryProcessExecutor;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.ExecutableOperator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.DnfFilterOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.MergeSingleFilterOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.RemoveNotOptimizer;

import java.time.ZoneId;

/**
 * provide a integration method for other user.
 */
public class QueryProcessor {

  private IQueryProcessExecutor executor;

  public QueryProcessor(IQueryProcessExecutor executor) {
    this.executor = executor;
  }

  public IQueryProcessExecutor getExecutor() {
    return executor;
  }

  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr)
      throws QueryProcessorException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    return parseSQLToPhysicalPlan(sqlStr, config.getZoneID());
  }

  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, ZoneId zoneId)
      throws QueryProcessorException {
    Operator operator = parseCommandToOperator(sqlStr, zoneId);
    operator = logicalOptimize(operator, executor);
    PhysicalGenerator physicalGenerator = new PhysicalGenerator(executor);
    PhysicalPlan qp = physicalGenerator.transformToPhysicalPlan(operator);
    return qp;
  }

  /**
   * Convert command string to Operator which type is Executable Operator
   *
   * @param command
   *            - input command
   * @return - ExecutableOperator has several subclass:Query/Insert/Delete/Update/Author
   */
  private ExecutableOperator parseCommandToOperator(String command, ZoneId zoneId) {
    LogicalGenerator generator = new LogicalGenerator(zoneId);
    return generator.getLogicalPlan(command);
  }

  /**
   * given an unoptimized logical operator tree and return a optimized result.
   *
   * @param operator
   *            unoptimized logical operator
   * @param executor
   * @return optimized logical operator
   * @throws LogicalOptimizeException
   *             exception in logical optimizing
   */
  private Operator logicalOptimize(Operator operator, IQueryProcessExecutor executor)
      throws LogicalOperatorException {
    switch (operator.getType()) {
      case AUTHOR:
      case METADATA:
      case SET_STORAGE_GROUP:
      case CREATE_TIMESERIES:
      case DELETE_TIMESERIES:
      case PROPERTY:
      case LOADDATA:
      case INSERT:
      case INDEX:
      case INDEXQUERY:
        return operator;
      case QUERY:
      case UPDATE:
      case DELETE:
        ExecutableOperator root = (ExecutableOperator) operator;
        return optimizeExecutableOperator(root, executor);
      default:
        throw new LogicalOperatorException("unknown operator type:" + operator.getType());
    }
  }

  /**
   * given an unoptimized select-from-where operator and return an optimized result.
   *
   * @param root
   *            unoptimized select-from-where operator
   * @param executor
   * @return optimized select-from-where operator
   * @throws LogicalOptimizeException
   *             exception in ExecutableOperator optimizing
   */
  private ExecutableOperator optimizeExecutableOperator(ExecutableOperator root, IQueryProcessExecutor executor)
      throws LogicalOperatorException {
    ConcatPathOptimizer concatPathOptimizer = new ConcatPathOptimizer(executor);
    root = (ExecutableOperator) concatPathOptimizer.transform(root);
    FilterOperator filter = root.getFilterOperator();
    if (filter == null) {
      return root;
    }
    RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
    filter = removeNot.optimize(filter);
    DnfFilterOptimizer dnf = new DnfFilterOptimizer();
    filter = dnf.optimize(filter);
    MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
    filter = merge.optimize(filter);
    root.setFilterOperator(filter);
    return root;
  }

}
