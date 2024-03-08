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

package org.apache.iotdb.db.queryengine.plan.expression.visitor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.GroupByLevelHelper;
import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.queryengine.plan.expression.leaf.TimestampOperand;

import java.util.function.UnaryOperator;

import static org.apache.iotdb.db.queryengine.plan.analyze.ExpressionUtils.reconstructTimeSeriesOperand;

public class ReplaceRawPathWithGroupedPathVisitor
    extends ReconstructVisitor<ReplaceRawPathWithGroupedPathVisitor.Context> {

  @Override
  public Expression visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Context context) {
    PartialPath rawPath = timeSeriesOperand.getPath();
    PartialPath groupedPath = context.transform(rawPath);
    return reconstructTimeSeriesOperand(timeSeriesOperand, groupedPath);
  }

  @Override
  public Expression visitTimeStampOperand(TimestampOperand timestampOperand, Context context) {
    return timestampOperand;
  }

  @Override
  public Expression visitConstantOperand(ConstantOperand constantOperand, Context context) {
    return constantOperand;
  }

  public static class Context {

    private final GroupByLevelHelper.RawPathToGroupedPathMap rawPathToGroupedPathMap;
    private final UnaryOperator<PartialPath> pathTransformer;

    public Context(
        GroupByLevelHelper.RawPathToGroupedPathMap rawPathToGroupedPathMap,
        UnaryOperator<PartialPath> pathTransformer) {
      this.rawPathToGroupedPathMap = rawPathToGroupedPathMap;
      this.pathTransformer = pathTransformer;
    }

    private PartialPath transform(PartialPath path) {
      if (rawPathToGroupedPathMap.containsKey(path)) {
        return rawPathToGroupedPathMap.get(path);
      }
      PartialPath groupedPath = pathTransformer.apply(path);
      rawPathToGroupedPathMap.put(path, groupedPath);
      return groupedPath;
    }
  }
}
