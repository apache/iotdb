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
import org.apache.iotdb.db.qp.physical.crud.GroupByTimeFillPlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class GroupByFillQueryOperator extends GroupByQueryOperator {

  private Map<TSDataType, IFill> fillTypes;


  @Override
  public PhysicalPlan transform2PhysicalPlan(int fetchSize, PhysicalGenerator generator)
      throws QueryProcessException {
    GroupByTimeFillPlan plan = new GroupByTimeFillPlan();
    super.setPlanValues(plan);
    plan.setFillType(getFillTypes());
    for (String aggregation : plan.getAggregations()) {
      if (!SQLConstant.LAST_VALUE.equals(aggregation)) {
        throw new QueryProcessException("Group By Fill only support last_value function");
      }
    }
    return doOptimization(plan, generator, fetchSize);
  }


  public Map<TSDataType, IFill> getFillTypes() {
    return fillTypes;
  }

  public void setFillTypes(Map<TSDataType, IFill> fillTypes) {
    this.fillTypes = fillTypes;
  }
}
