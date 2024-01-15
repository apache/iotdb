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

package org.apache.iotdb.db.queryengine.plan.expression.visitor.logical;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;

import static org.apache.iotdb.tsfile.utils.Preconditions.checkArgument;

public class PredicateCanPushDownToSourceChecker extends LogicalAndVisitor<String> {

  @Override
  public Boolean visitIsNullExpression(
      IsNullExpression isNullExpression, String checkedSourceSymbol) {
    if (!isNullExpression.isNot()) {
      // IS NULL cannot be pushed down
      return Boolean.FALSE;
    }
    return process(isNullExpression.getExpression(), checkedSourceSymbol);
  }

  @Override
  public Boolean visitTimeSeriesOperand(
      TimeSeriesOperand timeSeriesOperand, String checkedSourceSymbol) {
    PartialPath path = timeSeriesOperand.getPath();

    checkArgument(path instanceof MeasurementPath);
    // check if the predicate only contains checkedSourceSymbol
    if (((MeasurementPath) path).isUnderAlignedEntity()) {
      return path.getDevice().equals(checkedSourceSymbol);
    } else {
      return path.getFullPath().equals(checkedSourceSymbol);
    }
  }

  @Override
  public Boolean visitLeafOperand(LeafOperand leafOperand, String checkedSourceSymbol) {
    return Boolean.TRUE;
  }
}
