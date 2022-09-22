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

import org.apache.iotdb.db.mpp.common.NodeRef;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.AssignmentStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.DeclareStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodegenSimpleRow;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CodegenContext {
  private List<DeclareStatement> intermediateVariables;
  private List<AssignmentStatement> assignmentStatements;
  private final Map<String, List<InputLocation>> inputLocations;
  private final List<TSDataType> inputDataTypes;
  private Map<String, String> inputNameToVarName;
  private final List<Expression> outputExpression;
  private Map<Expression, ExpressionNode> expressionToNode;
  private List<UDTFExecutor> udtfExecutors;
  private List<CodegenSimpleRow> udtfRows;
  private final Map<NodeRef<Expression>, TSDataType> expressionTypes;
  private UDTFContext udtfContext;

  private int udtfIndex = 0;

  public void setUdtfContext(UDTFContext udtfContext) {
    this.udtfContext = udtfContext;
  }

  public void setIsExpressionGeneratedSuccess(List<Boolean> isExpressionGeneratedSuccess) {
    this.isExpressionGeneratedSuccess = isExpressionGeneratedSuccess;
  }

  private List<Boolean> isExpressionGeneratedSuccess;

  private long uniqueIndex;

  public CodegenContext(
      Map<String, List<InputLocation>> inputLocations,
      List<TSDataType> inputDataTypes,
      List<Expression> outputExpressions,
      Expression filterExpression,
      Map<NodeRef<Expression>, TSDataType> expressionTypes) {
    init();

    this.inputLocations = inputLocations;
    this.inputDataTypes = inputDataTypes;
    this.outputExpression = outputExpressions;
    this.expressionTypes = expressionTypes;
  }

  public void init() {
    this.expressionToNode = new HashMap<>();
    this.udtfRows = new ArrayList<>();
    this.udtfExecutors = new ArrayList<>();
    this.inputNameToVarName = new HashMap<>();
    this.intermediateVariables = new ArrayList<>();
    this.assignmentStatements = new ArrayList<>();
  }

  public void addInputVarNameMap(String inputName, String varName) {
    inputNameToVarName.put(inputName, varName);
  }

  public String getVarName(String inputName) {
    return inputNameToVarName.get(inputName);
  }

  public Map<String, String> getInputNameToVarName() {
    return inputNameToVarName;
  }

  public boolean isExpressionExisted(Expression expression) {
    return expressionToNode.containsKey(expression);
  }

  public void addExpression(Expression expression, ExpressionNode ExpressionNode) {
    if (!expressionToNode.containsKey(expression)) {
      expressionToNode.put(expression, ExpressionNode);
    }
  }

  public Map<String, TSDataType> getOutputName2TypeMap() {
    LinkedHashMap<String, TSDataType> outputName2TypeMap = new LinkedHashMap<>();
    for (Expression expression : outputExpression) {
      if (!expressionToNode.containsKey(expression)) {
        outputName2TypeMap.put("non-existVariable", TSDataType.BOOLEAN);
        continue;
      }
      outputName2TypeMap.put(
          expressionToNode.get(expression).getNodeName(),
          expressionTypes.get(NodeRef.of(expression)));
    }
    return outputName2TypeMap;
  }

  public ExpressionNode getExpressionNode(Expression expression) {
    if (expressionToNode.containsKey(expression)) {
      return expressionToNode.get(expression);
    }
    return null;
  }

  public void addUdtfExecutor(UDTFExecutor executor) {
    udtfExecutors.add(executor);
  }

  public void addUdtfInput(CodegenSimpleRow input) {
    udtfRows.add(input);
  }

  public String uniqueVarName() {
    return "var" + (uniqueIndex++);
  }

  public String uniqueVarName(String prefix) {
    return prefix + (uniqueIndex++);
  }

  public int getUdtfIndex() {
    udtfIndex++;
    return udtfIndex - 1;
  }

  public void addOutputExpr(Expression expression) {
    outputExpression.add(expression);
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

  public UDTFExecutor[] getUdtfExecutors() {
    return udtfExecutors.toArray(new UDTFExecutor[0]);
  }

  public CodegenSimpleRow[] getUdtfRows() {
    return udtfRows.toArray(new CodegenSimpleRow[0]);
  }

  public Map<String, List<InputLocation>> getInputLocations() {
    return inputLocations;
  }

  public List<Expression> getOutputExpression() {
    return outputExpression;
  }

  public List<TSDataType> getOutputDataTypes() {
    return outputExpression.stream()
        .map(expression -> expressionTypes.get(NodeRef.of(expression)))
        .collect(Collectors.toList());
  }

  public boolean isExpressionInput(Expression expression) {
    return inputLocations.containsKey(expression.getExpressionString());
  }

  public void addIntermediateVariable(DeclareStatement declareStatement) {
    this.intermediateVariables.add(
        new DeclareStatement("boolean", declareStatement.getVarName() + "IsNull"));
    this.intermediateVariables.add(declareStatement);
  }

  public void addAssignmentStatement(AssignmentStatement assignmentStatement) {
    this.assignmentStatements.add(assignmentStatement);
  }

  public List<DeclareStatement> getIntermediateVariables() {
    return intermediateVariables;
  }

  public List<AssignmentStatement> getAssignmentStatements() {
    return assignmentStatements;
  }

  public Map<NodeRef<Expression>, TSDataType> getExpressionTypes() {
    return expressionTypes;
  }

  public List<TSDataType> getInputDataTypes() {
    return inputDataTypes;
  }

  public UDTFExecutor getExecutorByFunctionExpression(FunctionExpression functionExpression) {
    return udtfContext.getExecutorByFunctionExpression(functionExpression);
  }

  public TSDataType inferType(Expression expression) {
    return expressionTypes.get(NodeRef.of(expression));
  }
}
