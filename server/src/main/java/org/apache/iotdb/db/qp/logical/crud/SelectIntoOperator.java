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

package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.exception.query.LogicalOperatorException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.crud.SelectIntoPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;

import java.util.List;

public class SelectIntoOperator extends Operator {

  private QueryOperator queryOperator;

  private List<PartialPath> intoPaths;

  public SelectIntoOperator() {
    super(SQLConstant.TOK_SELECT_INTO);
    operatorType = OperatorType.SELECT_INTO;
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    QueryPlan queryPlan = (QueryPlan) queryOperator.generatePhysicalPlan(generator);
    if (intoPaths.size() != queryPlan.getPaths().size()) {
      throw new QueryProcessException(
          "select into: the number of source paths and the number of target paths should be the same.");
    }
    return new SelectIntoPlan(queryPlan, intoPaths);
  }

  public void check() throws LogicalOperatorException {
    queryOperator.check();

    // TODO: check query plan type
  }

  public void setQueryOperator(QueryOperator queryOperator) {
    this.queryOperator = queryOperator;
  }

  public QueryOperator getQueryOperator() {
    return queryOperator;
  }

  public void setIntoPaths(List<PartialPath> intoPaths) {
    this.intoPaths = intoPaths;
  }
}
