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
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.IdentityExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.expressionnode.ReturnValueExpressionNode;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.AssignmentStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.DeclareStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.IfStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.statements.MethodCallStatement;
import org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodeGenEvaluatorBaseClass;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * For all output expressions which can be dealt with codegen, we will generate a class extending
 * {@link CodeGenEvaluatorBaseClass}. The generated code will implement several abstract method in
 * {@link CodeGenEvaluatorBaseClass}, and declare all input variables and intermediate variables as
 * its field.
 *
 * <p>for example: select (a + b) * c from root.sg.d1;
 *
 * <pre>{@code
 * private int a;
 * private boolean aIsNull;
 * private float b;
 * private boolean bIsNull;
 * private double c;
 * private boolean cIsNull;
 * private double var1;
 * private boolean var1IsNull;
 * private double var2;
 * private boolean var2IsNull;
 *
 * protected void updateInputVariables(int i){
 *   if(valueColumns[0].isNull(i)){
 *     aIsNull = true;
 *   } else {
 *     a = valueColumns[0].getInt(i);
 *     aIsNull = false;
 *   }
 *   if(valueColumns[1].isNull(i)){
 *     bIsNull = true;
 *   } else {
 *     b = valueColumns[1].getFloat(i);
 *     bIsNull = false;
 *   }
 *   if(valueColumns[2].isNull(i)){
 *     cIsNull = true;
 *   } else {
 *     c = valueColumns[2].getFloat(i);
 *     cIsNull = false;
 *   }
 * }
 *
 * protected void evaluateByRow(int i){
 *   updateInputVariables(i);
 *   if(aIsNull || bIsNull){
 *     var1IsNull = true;
 *   } else {
 *     var1IsNull = false;
 *     var1 = a+b;
 *   }
 *   if(var2IsNull || cIsNull){
 *     var2IsNull = true;
 *   }else{
 *     var2 = var2 * c;
 *     var2IsNull = false;
 *   }
 *   if(var2IsNull){
 *     outputColumns[0].appendNull();
 *   }else{
 *     outputColumns[0].writeDouble(var2);
 *   }
 * }
 * }</pre>
 *
 * <p>Obviously, need to override two methods {@code CodeGenEvaluatorBaseClass#evaluateByRow(int)}
 * {@code CodeGenEvaluatorBaseClass#updateInputVariables(int)}
 */
public class CodegenEvaluatorImpl implements CodegenEvaluator {
  private final CodegenVisitor codegenVisitor;
  private final List<Boolean> generatedSuccess;
  private final CodegenContext codegenContext;
  private boolean scriptFinished;
  private CodeGenEvaluatorBaseClass codegenEvaluator;

  IClassBodyEvaluator classBodyEvaluator;

  public CodegenEvaluatorImpl(CodegenContext codegenContext) {
    codegenVisitor = new CodegenVisitor();
    generatedSuccess = new ArrayList<>();
    this.codegenContext = codegenContext;
    scriptFinished = false;
  }

  private String constructCode() {
    parseOutputExpressions();

    StringBuilder code = new StringBuilder();

    generateFieldDeclareStatements(code);
    generateUpdateInputVariables(code);
    generateEvaluateRow(code);

    return code.toString();
  }

  private void generateFieldDeclareStatements(StringBuilder code) {
    for (DeclareStatement declareStatement : codegenContext.getIntermediateVariables()) {
      // declare all variable and variableIsNull to sign whether variable is null
      code.append("private ").append(declareStatement.toCode());
    }
  }

  private void parseOutputExpressions() {
    // add expressions, this will generate variable declare and assignments
    for (Expression expression : codegenContext.getOutputExpression()) {
      boolean success = codegenVisitor.visitExpression(expression, codegenContext);
      generatedSuccess.add(success);
    }

    codegenContext.setIsExpressionGeneratedSuccess(generatedSuccess);
  }

  private void generateUpdateInputVariables(StringBuilder code) {
    // get all input variables and their position
    List<String> parameterNames = getParameterNames();

    // generate updateInputVariables() method
    code.append("protected void updateInputVariables(int i){\n");

    for (int i = 0; i < parameterNames.size(); ++i) {
      String varName = parameterNames.get(i);
      IfStatement ifStatement = new IfStatement(true);
      String instanceName = "valueColumns[" + i + "]";
      String methodName =
          "get" + tsDatatypeToPrimaryType(codegenContext.getInputDataTypes().get(i));
      ifStatement.setCondition(new IdentityExpressionNode(instanceName + ".isNull(i)"));
      ifStatement
          .addIfBodyStatement(
              new AssignmentStatement(varName + "IsNull", new ConstantExpressionNode("true")))
          .addElseBodyStatement(
              new AssignmentStatement(varName + "IsNull", new ConstantExpressionNode("false")))
          .addElseBodyStatement(
              new AssignmentStatement(
                  varName, new ReturnValueExpressionNode(instanceName, methodName, "i")));
      code.append(ifStatement.toCode());
    }
    code.append("}\n");
  }

  private void generateEvaluateRow(StringBuilder code) {
    // generate evaluateByRow()
    code.append("protected void evaluateByRow(int i){\n");
    code.append("updateInputVariables(i);\n");

    List<AssignmentStatement> assignmentStatements = codegenContext.getAssignmentStatements();
    for (AssignmentStatement assignmentStatement : assignmentStatements) {
      IfStatement ifStatement = new IfStatement(true);
      ifStatement.setCondition(assignmentStatement.getNullCondition());
      ifStatement
          .addIfBodyStatement(
              new AssignmentStatement(
                  assignmentStatement.getVarName() + "IsNull", new ConstantExpressionNode("true")))
          .addElseBodyStatement(
              new AssignmentStatement(
                  assignmentStatement.getVarName() + "IsNull", new ConstantExpressionNode("false")))
          .addElseBodyStatement(assignmentStatement);

      code.append(ifStatement.toCode());
    }

    // store variable in columnBuilder
    ArrayList<Map.Entry<String, TSDataType>> pairs =
        new ArrayList<>(codegenContext.getOutputName2TypeMap().entrySet());
    for (int i = 0; i < pairs.size(); i++) {
      if (!generatedSuccess.get(i)) {
        continue;
      }
      Map.Entry<String, TSDataType> output = pairs.get(i);
      IfStatement ifStatement = new IfStatement(true);
      ifStatement.setCondition(new IdentityExpressionNode(output.getKey() + "IsNull"));
      String methodName = "write" + tsDatatypeToPrimaryType(output.getValue());
      ifStatement.addIfBodyStatement(
          new MethodCallStatement("outputColumns[" + i + "]", "appendNull"));
      ifStatement.addElseBodyStatement(
          new MethodCallStatement("outputColumns[" + i + "]", methodName, output.getKey()));

      code.append(ifStatement.toCode());
    }
    code.append("}");
  }

  private String tsDatatypeToPrimaryType(TSDataType tsDataType) {
    switch (tsDataType) {
      case INT32:
        return "Int";
      case INT64:
        return "Long";
      case FLOAT:
        return "Float";
      case DOUBLE:
        return "Double";
      case BOOLEAN:
        return "Boolean";
      default:
        throw new UnsupportedOperationException();
    }
  }

  private List<String> getParameterNames() {
    Map<String, List<InputLocation>> inputLocations = codegenContext.getInputLocations();
    // the inputLocations map is input name -> column index, we need to reverse key-value and sort
    Map<Integer, String> columnToInputNameMap =
        inputLocations.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getValue().get(0).getValueColumnIndex(), Map.Entry::getKey));

    TreeMap<Integer, String> columnToInputNameTreeMap = new TreeMap<>(columnToInputNameMap);

    List<String> parameterNames =
        columnToInputNameTreeMap.values().stream()
            .map(codegenContext::getVarName)
            .collect(Collectors.toList());

    return parameterNames;
  }

  @Override
  public void generateEvaluatorClass() throws Exception {
    if (scriptFinished) {
      return;
    }

    codegenContext.init();
    String code = constructCode();

    classBodyEvaluator =
        CompilerFactoryFactory.getDefaultCompilerFactory(
                CodegenEvaluatorImpl.class.getClassLoader())
            .newClassBodyEvaluator();

    // set imports
    classBodyEvaluator.setDefaultImports(
        "org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodegenSimpleRow",
        "org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor");

    classBodyEvaluator.setExtendedClass(CodeGenEvaluatorBaseClass.class);

    classBodyEvaluator.cook(code);
    codegenEvaluator = (CodeGenEvaluatorBaseClass) classBodyEvaluator.getClazz().newInstance();
    codegenEvaluator.setOutputExpressionGenerateSuccess(generatedSuccess);
    codegenEvaluator.setOutputDataTypes(codegenContext.getOutputDataTypes());
    codegenEvaluator.setExecutors(codegenContext.getUdtfExecutors());
    codegenEvaluator.setRows(codegenContext.getUdtfRows());

    scriptFinished = true;
  }

  @Override
  public Column[] evaluate(TsBlock inputTsBlock) {
    return codegenEvaluator.evaluate(inputTsBlock);
  }
}
