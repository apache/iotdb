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

package org.apache.iotdb.db.mpp.execution.operator.process.codegen;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.binary.BinaryExpression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.ConstantOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimestampOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.expression.ternary.BetweenExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.InExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.IsNullExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.LogicNotExpression;
import org.apache.iotdb.db.mpp.plan.expression.unary.NegationExpression;

public interface CodegenVisitor {

  boolean expressionVisitor(Expression expression);

  boolean logicNotExpressionVisitor(LogicNotExpression logicNotExpression);

  boolean negationExpressionVisitor(NegationExpression negationExpression);

  boolean binaryExpressionVisitor(BinaryExpression binaryExpression);

  boolean isNullExpressionVisitor(IsNullExpression isNullExpression);

  //  boolean ternaryExpressionVisitor(TernaryExpression ternaryExpression);

  boolean betweenExpressionVisitor(BetweenExpression betweenExpression);

  //  boolean leafOperandVisitor(LeafOperand leafOperand) ;

  boolean constantOperandVisitor(ConstantOperand constantOperand);

  boolean timeSeriesOperandVisitor(TimeSeriesOperand timeSeriesOperand);

  boolean timestampOperandVisitor(TimestampOperand timestampOperand);

  //  boolean mappableFunctionExpressionVisitor(FunctionExpression expression);

  boolean functionExpressionVisitor(FunctionExpression functionExpression);

  boolean inExpressionVisitor(InExpression inExpression);
}
