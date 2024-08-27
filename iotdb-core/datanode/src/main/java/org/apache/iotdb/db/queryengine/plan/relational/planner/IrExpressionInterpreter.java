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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import com.google.common.collect.ImmutableMap;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.NodeRef;
import org.apache.iotdb.db.queryengine.plan.relational.function.InterpretedFunctionInvoker;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeCoercion;
import org.apache.tsfile.read.common.type.Type;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class IrExpressionInterpreter {

  private final Expression expression;
  private final PlannerContext plannerContext;
  private final Metadata metadata;
  private final LiteralInterpreter literalInterpreter;
  private final LiteralEncoder literalEncoder;
  private final SessionInfo session;
  private final Map<NodeRef<Expression>, Type> expressionTypes;
  private final InterpretedFunctionInvoker functionInvoker;
  private final TypeCoercion typeCoercion;

  private final IdentityHashMap<InListExpression, Set<?>> inListCache = new IdentityHashMap<>();


  public IrExpressionInterpreter(Expression expression, PlannerContext plannerContext, SessionInfo session, Map<NodeRef<Expression>, Type> expressionTypes)
  {
    this.expression = requireNonNull(expression, "expression is null");
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    this.metadata = plannerContext.getMetadata();
    this.literalInterpreter = new LiteralInterpreter(plannerContext, session);
    this.literalEncoder = new LiteralEncoder(plannerContext);
    this.session = requireNonNull(session, "session is null");
    this.expressionTypes = ImmutableMap.copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
    verify(expressionTypes.containsKey(NodeRef.of(expression)));
    this.functionInvoker = new InterpretedFunctionInvoker(plannerContext.getFunctionManager());
    this.typeCoercion = new TypeCoercion(plannerContext.getTypeManager()::getType);
  }

  public static Object evaluateConstantExpression(Expression expression, PlannerContext plannerContext, SessionInfo session)
  {
    Map<NodeRef<Expression>, Type> types = new IrTypeAnalyzer(plannerContext).getTypes(session, TypeProvider.empty(), expression);
    return new IrExpressionInterpreter(expression, plannerContext, session, types).evaluate();
  }

  public Object evaluate()
  {
    Object result = new Visitor(false).processWithExceptionHandling(expression, null);
    verify(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");
    return result;
  }

  public Object evaluate(SymbolResolver inputs)
  {
    Object result = new Visitor(false).processWithExceptionHandling(expression, inputs);
    verify(!(result instanceof Expression), "Expression interpreter returned an unresolved expression");
    return result;
  }

  public Object optimize(SymbolResolver inputs)
  {
    return new Visitor(true).processWithExceptionHandling(expression, inputs);
  }
}
