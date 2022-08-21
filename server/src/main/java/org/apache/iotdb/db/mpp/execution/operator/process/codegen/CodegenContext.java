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

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ConstantExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.NewObjectsStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.ReturnStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.Statement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodegenSimpleRow;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class CodegenContext {
  private final List<Statement> codes;

  private final Set<Expression> inputExpr;

  private final Set<Expression> constExpr;

  private final List<Expression> outputExpr;

  private final Map<Expression, Class<?>> expressionClass;

  private final Map<Expression, ExpressionNode> expressionToNode;

  private Map<String, UDTFExecutor> udtfExecutors;

  private Map<String, CodegenSimpleRow> udtfInputs;

  public CodegenContext() {
    this.codes = new ArrayList<>();
    this.expressionToNode = new HashMap<>();
    this.inputExpr = new HashSet<>();
    this.outputExpr = new ArrayList<>();
    this.expressionClass = new HashMap<>();
    this.constExpr = new HashSet<>();
    this.udtfInputs = new HashMap<>();
    this.udtfExecutors = new HashMap<>();
  }

  private long uniqueIndex;

  public boolean isExpressionExisted(Expression expression) {
    return expressionToNode.containsKey(expression);
  }

  public void addExpression(
      Expression expression, ExpressionNode ExpressionNode, TSDataType tsDataType) {
    if (!expressionToNode.containsKey(expression)) {
      expressionToNode.put(expression, ExpressionNode);
      expressionClass.put(expression, tsDatatypeToClass(tsDataType));
    }
  }

  public ExpressionNode getExpressionNode(Expression expression) {
    if (expressionToNode.containsKey(expression)) {
      return expressionToNode.get(expression);
    }
    return null;
  }

  public void addInputExpr(Expression expression) {
    inputExpr.add(expression);
  }

  public void addConstExpr(Expression expression) {
    constExpr.add(expression);
  }

  public void addUdtfExecutor(String executorName, UDTFExecutor executor) {
    udtfExecutors.put(executorName, executor);
  }

  public void addUdtfInput(String rowName, CodegenSimpleRow input) {
    if (Objects.isNull(udtfInputs)) {
      udtfInputs = new HashMap<>();
    }
    udtfInputs.put(rowName, input);
  }

  public boolean isInputOrConstant(Expression expression) {
    if (inputExpr.contains(expression)) {
      return true;
    }
    return constExpr.contains(expression);
  }

  public void addCode(Statement statement) {
    codes.add(statement);
  }

  public String uniqueVarName() {
    return "var" + (uniqueIndex++);
  }

  public String uniqueVarName(String prefix) {
    return prefix + (uniqueIndex++);
  }

  public void addOutputExpr(Expression expression) {
    outputExpr.add(expression);
  }

  public void generateReturnStatement() {
    String retValueVarName = uniqueVarName();
    NewObjectsStatement retValueStatement =
        new NewObjectsStatement(retValueVarName, outputExpr.size());

    for (Expression expression : outputExpr) {
      if (Objects.isNull(expression)) {
        retValueStatement.addRetValue(null);
      } else {
        retValueStatement.addRetValue(expressionToNode.get(expression));
      }
    }
    codes.add(retValueStatement);
    codes.add(new ReturnStatement(new ConstantExpressionNode(retValueVarName)));
  }

  public String toCode() {
    StringBuilder code = new StringBuilder();
    for (Statement statement : codes) {
      if (statement != null) code.append(statement.toCode());
    }
    return code.toString();
  }

  public static Class<?> tsDatatypeToClass(TSDataType tsDataType) {
    switch (tsDataType) {
      case INT32:
        return Integer.class;
      case INT64:
        return Long.class;
      case FLOAT:
        return Float.class;
      case DOUBLE:
        return Double.class;
      case BOOLEAN:
        return Boolean.class;
      case TEXT:
        return String.class;
      default:
        throw new UnSupportedDataTypeException(
            String.format("Data type %s is not supported for codegen.", tsDataType));
    }
  }

  public Set<Expression> getInputExpr() {
    return inputExpr;
  }

  public Class<?> getExpressionType(Expression expression) {
    if (!expressionClass.containsKey(expression)) {
      return null;
    }
    return expressionClass.get(expression);
  }

  public Map<String, UDTFExecutor> getUdtfExecutors() {
    return udtfExecutors;
  }

  public Map<String, CodegenSimpleRow> getUdtfInputs() {
    return udtfInputs;
  }
}
