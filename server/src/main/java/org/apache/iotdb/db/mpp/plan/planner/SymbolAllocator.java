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

package org.apache.iotdb.db.mpp.plan.planner;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.leaf.TimeSeriesOperand;
import org.apache.iotdb.db.mpp.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.HashMap;
import java.util.Map;

public class SymbolAllocator {

  private final Map<Symbol, TSDataType> symbols;
  private int nextId;

  public SymbolAllocator() {
    symbols = new HashMap<>();
  }

  public Symbol newSymbol(Expression expression, TSDataType type) {
    String nameHint = "expr";
    if (expression instanceof TimeSeriesOperand) {
      nameHint = "series";
    } else if (expression instanceof FunctionExpression) {
      FunctionExpression functionExpression = (FunctionExpression) expression;
      nameHint = functionExpression.getFunctionName();
    }
    return newSymbol(nameHint, type);
  }

  public Symbol newSymbol(String nameHint, TSDataType type) {
    nameHint = nameHint.toLowerCase();

    String unique = nameHint;

    Symbol symbol = new Symbol(unique);
    while (symbols.putIfAbsent(symbol, type) != null) {
      symbol = new Symbol(unique + "_" + nextId());
    }

    return symbol;
  }

  private int nextId() {
    return nextId++;
  }
}
