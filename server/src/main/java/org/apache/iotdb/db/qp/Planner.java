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
package org.apache.iotdb.db.qp;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.logical.crud.FilterOperator;
import org.apache.iotdb.db.qp.logical.crud.SFWOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.strategy.ParseDriver;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.qp.strategy.optimizer.ConcatPathOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.DnfFilterOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.MergeSingleFilterOptimizer;
import org.apache.iotdb.db.qp.strategy.optimizer.RemoveNotOptimizer;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.read.common.Path;

import java.time.ZoneId;
import java.util.Set;

/**
 * provide a integration method for other user.
 */
public class Planner {

  protected ParseDriver parseDriver;

  public Planner() {
    this.parseDriver = new ParseDriver();
  }

  @TestOnly
  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr)
      throws QueryProcessException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    return parseSQLToPhysicalPlan(sqlStr, config.getZoneID());
  }

  public PhysicalPlan parseSQLToPhysicalPlan(String sqlStr, ZoneId zoneId)
      throws QueryProcessException {
    Operator operator = parseDriver.parse(sqlStr, zoneId);
    operator = logicalOptimize(operator);
    PhysicalGenerator physicalGenerator = new PhysicalGenerator();
    return physicalGenerator.transformToPhysicalPlan(operator);
  }


  /**
   * given an unoptimized logical operator tree and return a optimized result.
   *
   * @param operator unoptimized logical operator
   * @return optimized logical operator
   * @throws LogicalOptimizeException exception in logical optimizing
   */
  protected Operator logicalOptimize(Operator operator)
      throws LogicalOperatorException {
    switch (operator.getType()) {
      case AUTHOR:
      case METADATA:
      case SET_STORAGE_GROUP:
      case DELETE_STORAGE_GROUP:
      case CREATE_TIMESERIES:
      case DELETE_TIMESERIES:
      case ALTER_TIMESERIES:
      case LOADDATA:
      case INSERT:
      case INDEX:
      case INDEXQUERY:
      case GRANT_WATERMARK_EMBEDDING:
      case REVOKE_WATERMARK_EMBEDDING:
      case TTL:
      case LOAD_CONFIGURATION:
      case SHOW:
      case LOAD_FILES:
      case REMOVE_FILE:
      case MOVE_FILE:
      case FLUSH:
      case MERGE:
      case TRACING:
      case CLEAR_CACHE:
      case NULL:
      case SHOW_MERGE_STATUS:
      case DELETE_PARTITION:
      case CREATE_SCHEMA_SNAPSHOT:
        return operator;
      case QUERY:
      case UPDATE:
      case DELETE:
        SFWOperator root = (SFWOperator) operator;
        return optimizeSFWOperator(root);
      default:
        throw new LogicalOperatorException(operator.getType().toString(), "");
    }
  }

  /**
   * given an unoptimized select-from-where operator and return an optimized result.
   *
   * @param root unoptimized select-from-where operator
   * @return optimized select-from-where operator
   * @throws LogicalOptimizeException exception in SFW optimizing
   */
  private SFWOperator optimizeSFWOperator(SFWOperator root)
      throws LogicalOperatorException {
    ConcatPathOptimizer concatPathOptimizer = getConcatPathOptimizer();
    root = (SFWOperator) concatPathOptimizer.transform(root);
    FilterOperator filter = root.getFilterOperator();
    if (filter == null) {
      return root;
    }
    Set<Path> pathSet = filter.getPathSet();
    RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
    filter = removeNot.optimize(filter);
    DnfFilterOptimizer dnf = new DnfFilterOptimizer();
    filter = dnf.optimize(filter);
    MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
    filter = merge.optimize(filter);
    root.setFilterOperator(filter);
    filter.setPathSet(pathSet);
    return root;
  }

  protected ConcatPathOptimizer getConcatPathOptimizer() {
    return new ConcatPathOptimizer();
  }
}
