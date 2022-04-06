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

package org.apache.iotdb.db.mpp.sql.statement.crud;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.common.filter.QueryFilter;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;
import org.apache.iotdb.db.mpp.sql.statement.component.FillComponent;
import org.apache.iotdb.db.qp.constant.SQLConstant;

import java.util.Arrays;

public class FillQueryStatement extends QueryStatement {

  FillComponent fillComponent;

  public FillQueryStatement() {
    super();
  }

  public FillComponent getFillComponent() {
    return fillComponent;
  }

  public void setFillComponent(FillComponent fillComponent) {
    this.fillComponent = fillComponent;
  }

  @Override
  public void selfCheck() {
    super.selfCheck();

    if (DisableAlign()) {
      throw new SemanticException("FILL doesn't support disable align clause.");
    }

    if (hasTimeSeriesGeneratingFunction() || hasUserDefinedAggregationFunction()) {
      throw new SemanticException("Fill functions are not supported in UDF queries.");
    }

    if (whereCondition == null || whereCondition.getQueryFilter() == null) {
      throw new SemanticException("FILL must be used with a WHERE clause");
    }

    QueryFilter queryFilter = whereCondition.getQueryFilter();
    if (!queryFilter.isLeaf()
        || queryFilter.getFilterType() != FilterConstant.FilterType.EQUAL
        || !Arrays.equals(
            SQLConstant.getSingleTimeArray(),
            whereCondition.getQueryFilter().getSinglePath().getNodes())) {
      throw new SemanticException("The condition of WHERE clause must be like time=constant");
    } else if (!queryFilter.isSingle()) {
      throw new SemanticException("Slice query must select a single time point");
    }
  }

  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitFillQuery(this, context);
  }
}
