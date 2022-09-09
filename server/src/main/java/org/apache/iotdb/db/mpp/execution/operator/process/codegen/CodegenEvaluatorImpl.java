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

import org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodegenSimpleRow;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CodegenEvaluatorImpl implements CodegenEvaluator {
  private final CodegenVisitor codegenVisitor;
  private IScriptEvaluator scriptEvaluator;
  private IScriptEvaluator filterEvaluator;
  private final List<Boolean> generatedSuccess;
  private List<Object> additionalParas;
  private List<Object> additionalFilterParas;
  private final CodegenContext codegenContext;
  private boolean scriptFinished;
  private boolean filterFinished;
  private boolean filterGeneratedSuccess;

  public CodegenEvaluatorImpl(CodegenContext codegenContext) {
    codegenVisitor = new CodegenVisitorImpl(codegenContext);
    generatedSuccess = new ArrayList<>();
    this.codegenContext = codegenContext;
    scriptFinished = false;
  }

  private void constructCode() {
    // add expressions, this will generate variable declare and assignments
    for (Expression expression : codegenContext.getOutputExpression()) {
      boolean success = codegenVisitor.expressionVisitor(expression);
      generatedSuccess.add(success);
    }

    // this will generate return statement
    codegenContext.setIsExpressionGeneratedSuccess(generatedSuccess);
    codegenContext.generateReturnStatement();
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

    parameterNames.add("timestamp");

    Map<String, UDTFExecutor> udtfExecutors = codegenContext.getUdtfExecutors();
    Map<String, CodegenSimpleRow> udtfInputs = codegenContext.getUdtfInputs();

    // deal with udtfExecutors and CodegenSimpleRows
    // they will be treated as additional parameters
    parameterNames.addAll(udtfExecutors.keySet());
    parameterNames.addAll(udtfInputs.keySet());
    return parameterNames;
  }

  private List<Class<?>> getParameterClasses() {
    List<Class<?>> parameterClasses =
        codegenContext.getInputDataTypes().stream()
            .map(CodegenContext::tsDatatypeToClass)
            .collect(Collectors.toList());

    parameterClasses.add(long.class);

    Map<String, UDTFExecutor> udtfExecutors = codegenContext.getUdtfExecutors();
    Map<String, CodegenSimpleRow> udtfInputs = codegenContext.getUdtfInputs();

    parameterClasses.addAll(Collections.nCopies(udtfExecutors.size(), UDTFExecutor.class));
    parameterClasses.addAll(Collections.nCopies(udtfInputs.size(), CodegenSimpleRow.class));

    return parameterClasses;
  }

  @Override
  public void generateFilterEvaluator() throws Exception {
    if (filterFinished) {
      return;
    }

    codegenContext.init();
    // this should always be true
    Expression filterExpression = codegenContext.getFilterExpression();
    if (Objects.isNull(filterExpression)) {
      filterFinished = true;
      filterGeneratedSuccess = false;
      return;
    }
    boolean success = codegenVisitor.expressionVisitor(filterExpression);
    if (!success) {
      filterFinished = true;
      filterGeneratedSuccess = false;
      return;
    }
    codegenContext.generateReturnFilter();

    Map<String, UDTFExecutor> udtfExecutors = codegenContext.getUdtfExecutors();
    Map<String, CodegenSimpleRow> udtfInputs = codegenContext.getUdtfInputs();

    additionalFilterParas = new ArrayList<>();
    additionalFilterParas.addAll(udtfExecutors.values());
    additionalFilterParas.addAll(udtfInputs.values());

    filterEvaluator =
        CompilerFactoryFactory.getDefaultCompilerFactory(
                CodegenEvaluatorImpl.class.getClassLoader())
            .newScriptEvaluator();

    // set imports
    filterEvaluator.setDefaultImports(
        "org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodegenSimpleRow",
        "org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.UDTFCaller",
        "org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor",
        "java.util.HashSet");

    Class<?> outputType = Boolean.class;
    filterEvaluator.setReturnType(outputType);

    List<String> parameterNames = getParameterNames();
    List<Class<?>> parameterClasses = getParameterClasses();

    filterEvaluator.setParameters(
        parameterNames.toArray(new String[0]), parameterClasses.toArray(new Class[0]));

    String code = codegenContext.toCode();
    filterEvaluator.cook(code);

    filterFinished = true;
    filterGeneratedSuccess = true;
  }

  @Override
  public void generateScriptEvaluator() throws Exception {
    if (scriptFinished) {
      return;
    }

    codegenContext.init();
    constructCode();
    codegenContext.setIsExpressionGeneratedSuccess(generatedSuccess);

    Map<String, UDTFExecutor> udtfExecutors = codegenContext.getUdtfExecutors();
    Map<String, CodegenSimpleRow> udtfInputs = codegenContext.getUdtfInputs();

    additionalParas = new ArrayList<>();
    additionalParas.addAll(udtfExecutors.values());
    additionalParas.addAll(udtfInputs.values());

    scriptEvaluator =
        CompilerFactoryFactory.getDefaultCompilerFactory(
                CodegenEvaluatorImpl.class.getClassLoader())
            .newScriptEvaluator();

    // set imports
    scriptEvaluator.setDefaultImports(
        "org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodegenSimpleRow",
        "org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.UDTFCaller",
        "org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor",
        "java.util.HashSet");

    // set parameter names and types
    // udtfExecutor and Row are treated as input
    Class<?> outputType = Object[].class;
    scriptEvaluator.setReturnType(outputType);

    List<String> parameterNames = getParameterNames();
    List<Class<?>> parameterClasses = getParameterClasses();

    scriptEvaluator.setParameters(
        parameterNames.toArray(new String[0]), parameterClasses.toArray(new Class<?>[0]));

    String code = codegenContext.toCode();

    scriptEvaluator.cook(code);
    scriptFinished = true;
  }

  public Column evaluateFilter(TsBlock input) throws InvocationTargetException {
    ArrayList<TSDataType> tsDataTypes = new ArrayList<>();
    tsDataTypes.add(TSDataType.BOOLEAN);
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(tsDataTypes);
    final ColumnBuilder filterColumnBuilder = tsBlockBuilder.getValueColumnBuilders()[0];
    TsBlock.TsBlockRowIterator inputIterator = input.getTsBlockRowIterator();

    while (inputIterator.hasNext()) {
      Object[] finalArgs = setArgs(inputIterator.next(), additionalFilterParas);
      Boolean output = (Boolean) filterEvaluator.evaluate(finalArgs);
      if (Objects.isNull(output)) {
        filterColumnBuilder.appendNull();
      } else {
        filterColumnBuilder.writeBoolean(output);
      }
    }
    return filterColumnBuilder.build();
  }

  @Override
  public Column[] evaluate(TsBlock inputTsBlock) throws InvocationTargetException {
    List<TSDataType> outputDataTypes = codegenContext.getOutputDataTypes();
    TsBlockBuilder tsBlockBuilder = new TsBlockBuilder(outputDataTypes);
    TsBlock.TsBlockRowIterator inputIterator = inputTsBlock.getTsBlockRowIterator();

    final ColumnBuilder[] columnBuilders = tsBlockBuilder.getValueColumnBuilders();
    int positionCount = columnBuilders.length;

    while (inputIterator.hasNext()) {
      Object[] finalArgs = setArgs(inputIterator.next(), additionalParas);
      Object[] outputs = (Object[]) scriptEvaluator.evaluate(finalArgs);
      for (int i = 0; i < positionCount; i++) {
        if (Objects.isNull(outputs[i])) {
          columnBuilders[i].appendNull();
        } else {
          columnBuilders[i].writeObject(outputs[i]);
        }
      }
    }

    Column[] retColumns = new Column[positionCount];
    for (int i = 0; i < positionCount; i++) {
      retColumns[i] = columnBuilders[i].build();
    }
    return retColumns;
  }

  @Override
  public List<Boolean> isGenerated() {
    return generatedSuccess;
  }

  @Override
  public Object[] accept(Object[] args, long timestamp) throws InvocationTargetException {
    Object[] finalArgs = setArgs(args, timestamp);
    return (Object[]) scriptEvaluator.evaluate(finalArgs);
  }

  private Object[] setArgs(Object[] args, long timestamp) {
    List<Object> finalArgs = Stream.of(args).collect(Collectors.toList());
    finalArgs.add(timestamp);
    finalArgs.addAll(additionalParas);
    return finalArgs.toArray(new Object[0]);
  }

  private Object[] setArgs(Object[] args, List<Object> additional) {
    Object[] finalArgs = Arrays.copyOf(args, args.length + additional.size());
    for (int i = 0; i < additional.size(); i++) {
      finalArgs[i + args.length] = additional.get(i);
    }
    return finalArgs;
  }

  @Override
  public boolean isFilterGeneratedSuccess() {
    return filterFinished && filterGeneratedSuccess;
  }
}
