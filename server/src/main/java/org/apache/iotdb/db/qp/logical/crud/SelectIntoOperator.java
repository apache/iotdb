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

import java.util.HashSet;
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
    return new SelectIntoPlan(
        queryPlan, queryOperator.getFromComponent().getPrefixPaths().get(0), intoPaths);
  }

  public void check() throws LogicalOperatorException {
    queryOperator.check();

    if (intoPaths.size() > new HashSet<>(intoPaths).size()) {
      throw new LogicalOperatorException(
          "select into: target paths in into clause should be different.");
    }

    if (queryOperator.isAlignByDevice()) {
      throw new LogicalOperatorException("select into: align by device clauses are not supported.");
    }

    // disable align
    if (!queryOperator.isAlignByTime()) {
      throw new LogicalOperatorException("select into: disable align clauses are not supported.");
    }

    if (queryOperator instanceof LastQueryOperator) {
      throw new LogicalOperatorException("select into: last clauses are not supported.");
    }

    if (queryOperator instanceof AggregationQueryOperator
        && !(queryOperator instanceof GroupByQueryOperator)) {
      throw new LogicalOperatorException("select into: aggregation queries are not supported.");
    }

    if (queryOperator.getSpecialClauseComponent() != null) {
      SpecialClauseComponent specialClauseComponent = queryOperator.getSpecialClauseComponent();
      if (specialClauseComponent.hasSlimit()) {
        throw new LogicalOperatorException("select into: slimit clauses are not supported.");
      }
      if (specialClauseComponent.getSeriesOffset() > 0) {
        throw new LogicalOperatorException("select into: soffset clauses are not supported.");
      }
      if (!specialClauseComponent.isAscending()) {
        throw new LogicalOperatorException(
            "select into: order by time desc clauses are not supported.");
      }
    }
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
