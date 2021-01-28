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

import java.util.List;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class GroupByLevelQueryOperator extends GroupByQueryOperator {

  private int level = -1;

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }


  @Override
  public PhysicalPlan transform2PhysicalPlan(int fetchSize,
      PhysicalGenerator generator) throws QueryProcessException {
    AggregationPlan plan;

    if (getUnit() > 0) {
      plan = new GroupByTimePlan();
      super.setPlanValues((GroupByTimePlan) plan);
    } else {
      plan = new AggregationPlan();
      super.setPlanValues(plan);
    }
    plan.setLevel(getLevel());

    try {
      if (!verifyAllAggregationDataTypesEqual(generator, this)) {
        throw new QueryProcessException("Aggregate among unmatched data types");
      }
    } catch (MetadataException e) {
      throw new QueryProcessException(e);
    }
    if (isAlignByDevice() && getLevel() >= 0) {
      throw new QueryProcessException("group by level does not support align by device now.");
    }
    return doOptimization(plan, generator, fetchSize);
  }


  private boolean verifyAllAggregationDataTypesEqual(PhysicalGenerator generator,
      QueryOperator queryOperator) throws MetadataException {
    List<String> aggregations = queryOperator.getSelectOperator().getAggregations();
    if (aggregations.isEmpty()) {
      return true;
    }

    List<PartialPath> paths = queryOperator.getSelectedPaths();
    List<TSDataType> dataTypes = generator.getSeriesTypes(paths);
    String aggType = aggregations.get(0);
    switch (aggType) {
      case SQLConstant.MIN_VALUE:
      case SQLConstant.MAX_VALUE:
      case SQLConstant.AVG:
      case SQLConstant.SUM:
        return dataTypes.stream().allMatch(dataTypes.get(0)::equals);
      default:
        return true;
    }
  }
}
