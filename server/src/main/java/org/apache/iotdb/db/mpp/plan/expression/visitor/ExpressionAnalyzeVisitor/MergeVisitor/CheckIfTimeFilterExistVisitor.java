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

package org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionAnalyzeVisitor.MergeVisitor;

import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;

import java.util.List;

public class CheckIfTimeFilterExistVisitor extends MergeVisitor<Boolean, Void> {
  @Override
  Boolean merge(List<Boolean> childResults) {
    return childResults.stream().reduce(false, Boolean::logicalOr);
  }

  @Override
  public Boolean visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
    return false;
  }

  @Override
  public Boolean visitConstantOperand(ConstantOperand constantOperand, Void context) {
    return false;
  }

  @Override
  public Boolean visitIsNullExpression(IsNullExpression isNullExpression, Void context) {
    return false;
  }

  @Override
  public Boolean visitTimeStampOperand(TimestampOperand timestampOperand, Void context) {
    return true;
  }
}
