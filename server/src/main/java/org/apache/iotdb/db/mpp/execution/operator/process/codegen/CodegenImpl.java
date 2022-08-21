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
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IScriptEvaluator;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CodegenImpl implements Codegen {
  private final CodegenVisitor codegenVisitor;
  private IScriptEvaluator scriptEvaluator;
  private List<String> parameterNames;
  private List<Class<?>> parameterTypes;
  private final List<Boolean> generated;
  private List<Object> additionalParas;
  private TypeProvider typeProvider;
  private int inputArgNumber;
  private final CodegenContext codegenContext;

  public CodegenImpl(
      TypeProvider typeProvider, UDTFContext udtfContext, CodegenContext codegenContext) {
    this.typeProvider = typeProvider;
    codegenVisitor = new CodegenVisitorImpl(typeProvider, udtfContext, codegenContext);
    generated = new ArrayList<>();
    this.codegenContext = codegenContext;
  }

  @Override
  public Codegen setTypeProvider(TypeProvider typeProvider) {
    this.typeProvider = typeProvider;
    return this;
  }

  @Override
  public Codegen setInputs(List<String> paths, List<TSDataType> tsDataTypes) {
    parameterNames =
        paths.stream().map(path -> path.replace(".", "_")).collect(Collectors.toList());
    parameterTypes =
        tsDataTypes.stream().map(CodegenContext::tsDatatypeToClass).collect(Collectors.toList());

    parameterNames.add("timestamp");
    parameterTypes.add(Long.class);
    inputArgNumber = paths.size();
    return this;
  }

  @Override
  public boolean addExpression(Expression expression) {
    boolean success = expression.codegenAccept(codegenVisitor);
    if (success) {
      codegenContext.addOutputExpr(expression);
    } else {
      codegenContext.addOutputExpr(null);
    }
    generated.add(success);
    return success;
  }

  @Override
  public void generateScriptEvaluator() throws Exception {
    scriptEvaluator =
        CompilerFactoryFactory.getDefaultCompilerFactory(CodegenImpl.class.getClassLoader())
            .newScriptEvaluator();

    // set imports
    scriptEvaluator.setDefaultImports(
        "org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.CodegenSimpleRow",
        "org.apache.iotdb.db.mpp.execution.operator.process.codegen.utils.UDTFCaller",
        "org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFExecutor");

    // set parameter names and types
    // udtfExecutor and Row are treated as input
    Class<?> outputType = Object[].class;
    scriptEvaluator.setReturnType(outputType);

    Map<String, UDTFExecutor> udtfExecutors = codegenContext.getUdtfExecutors();
    Map<String, CodegenSimpleRow> udtfInputs = codegenContext.getUdtfInputs();

    // deal with udtfExecutors and CodegenSimpleRows
    // they will be treated as additional parameters
    parameterNames.addAll(udtfExecutors.keySet());
    parameterNames.addAll(udtfInputs.keySet());

    additionalParas = new ArrayList<>();
    additionalParas.addAll(udtfExecutors.values());
    additionalParas.addAll(udtfInputs.values());

    parameterTypes.addAll(Collections.nCopies(udtfExecutors.size(), UDTFExecutor.class));
    parameterTypes.addAll(Collections.nCopies(udtfInputs.size(), CodegenSimpleRow.class));

    scriptEvaluator.setParameters(
        parameterNames.toArray(new String[0]), parameterTypes.toArray(new Class<?>[0]));

    codegenContext.generateReturnStatement();

    String code = codegenContext.toCode();

    scriptEvaluator.cook(code);
  }

  @Override
  public List<Boolean> isGenerated() {
    return generated;
  }

  @Override
  public Object[] accept(Object[] args, long timestamp) throws InvocationTargetException {
    Object[] finalArgs = setArgs(args, timestamp);
    return (Object[]) scriptEvaluator.evaluate(finalArgs);
  }

  public Object[] setArgs(Object[] args, long timestamp) {
    List<Object> finalArgs = Stream.of(args).collect(Collectors.toList());
    finalArgs.add(timestamp);
    finalArgs.addAll(additionalParas);
    return finalArgs.toArray(new Object[0]);
  }
}
