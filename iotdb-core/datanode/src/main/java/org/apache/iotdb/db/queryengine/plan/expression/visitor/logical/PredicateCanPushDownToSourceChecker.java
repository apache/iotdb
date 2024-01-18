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

import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.LeafOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.unary.IsNullExpression;

import static org.apache.iotdb.tsfile.utils.Preconditions.checkArgument;

public class PredicateCanPushDownToSourceChecker
    extends LogicalAndVisitor<PredicateCanPushDownToSourceChecker.Context> {

  @Override
  public Boolean visitIsNullExpression(IsNullExpression isNullExpression, Context context) {
    if (!isNullExpression.isNot()) {
      // IS NULL cannot be pushed down
      return Boolean.FALSE;
    }
    return process(isNullExpression.getExpression(), context);
  }

  @Override
  public Boolean visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Context context) {
    PartialPath path = timeSeriesOperand.getPath();

    if (context.isBuildPlanUseTemplate()) {
      String measurement = path.getFullPath();
      if (context.isAligned()) {
        return ((AlignedPath) context.getCheckedSourcePath())
            .getMeasurementList()
            .contains(measurement);
      } else {
        return measurement.equals(context.getCheckedSourcePath().getMeasurement());
      }
    } else {
      checkArgument(path instanceof MeasurementPath);
      // check if the predicate only contains checkedSourceSymbol
      if (((MeasurementPath) path).isUnderAlignedEntity()) {
        return context.isAligned()
            && path.getDevice().equals(context.getCheckedSourcePath().getDevice());
      } else {
        return !context.isAligned()
            && path.getFullPath().equals(context.getCheckedSourcePath().getFullPath());
      }
    }
  }

  @Override
  public Boolean visitLeafOperand(LeafOperand leafOperand, Context context) {
    return Boolean.TRUE;
  }

  public static class Context {

    private final PartialPath checkedSourcePath;
    private final boolean isAligned;
    private final boolean isBuildPlanUseTemplate;

    public Context(PartialPath checkedSourcePath, boolean isBuildPlanUseTemplate) {
      this.checkedSourcePath = checkedSourcePath;
      this.isAligned = checkedSourcePath instanceof AlignedPath;
      this.isBuildPlanUseTemplate = isBuildPlanUseTemplate;
    }

    public PartialPath getCheckedSourcePath() {
      return checkedSourcePath;
    }

    public boolean isBuildPlanUseTemplate() {
      return isBuildPlanUseTemplate;
    }

    public boolean isAligned() {
      return isAligned;
    }
  }
}
