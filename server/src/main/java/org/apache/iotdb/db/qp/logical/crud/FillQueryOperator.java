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

import java.util.Map;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.FillQueryPlan;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class FillQueryOperator extends QueryOperator {

  private Map<TSDataType, IFill> fillTypes;

  public FillQueryOperator() {
    super(SQLConstant.TOK_QUERY);
  }


  public Map<TSDataType, IFill> getFillTypes() {
    return fillTypes;
  }

  public void setFillTypes(
      Map<TSDataType, IFill> fillTypes) {
    this.fillTypes = fillTypes;
  }

  @Override
  public PhysicalPlan convert(int fetchSize) throws QueryProcessException {
    if (hasUdf()) {
      throw new QueryProcessException("Fill functions are not supported in UDF queries.");
    }
    FillQueryPlan plan = new FillQueryPlan();
    FilterOperator timeFilter = getFilterOperator();
    if (!timeFilter.isSingle()) {
      throw new QueryProcessException("Slice query must select a single time point");
    }
    long time = Long.parseLong(((BasicFunctionOperator) timeFilter).getValue());
    plan.setQueryTime(time);
    plan.setFillType(getFillTypes());
    return plan;
  }
}
