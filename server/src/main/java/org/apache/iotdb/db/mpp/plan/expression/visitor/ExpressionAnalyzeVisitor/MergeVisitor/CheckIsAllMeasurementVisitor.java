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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.NullOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;

import java.util.List;

public class CheckIsAllMeasurementVisitor extends MergeVisitor<Void, Void> {
  @Override
  Void merge(List<Void> childResults) {
    return null;
  }

  @Override
  public Void visitTimeSeriesOperand(TimeSeriesOperand timeSeriesOperand, Void context) {
    PartialPath path = timeSeriesOperand.getPath();
    if (path.getNodes().length > 1
        || path.getFullPath().equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
      throw new SemanticException("the suffix paths can only be measurement or one-level wildcard");
    }
    return null;
  }

  @Override
  public Void visitTimeStampOperand(TimestampOperand timestampOperand, Void context) {
    // do nothing
    return null;
  }

  @Override
  public Void visitConstantOperand(ConstantOperand constantOperand, Void context) {
    // do nothing
    return null;
  }

  @Override
  public Void visitNullOperand(NullOperand nullOperand, Void context) {
    // do nothing
    return null;
  }
}
