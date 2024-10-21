/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.relational.analyzer.Field;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import org.apache.tsfile.read.common.type.Type;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SymbolAllocator {

  public static final String GROUP_KEY_SUFFIX = "gid";

  public static final String SEPARATOR = "$";

  private final Map<Symbol, Type> symbolMap;
  private int nextId;

  public SymbolAllocator() {
    symbolMap = new HashMap<>();
  }

  public Symbol newSymbol(Symbol symbolHint) {
    return newSymbol(symbolHint, null);
  }

  public Symbol newSymbol(Symbol symbolHint, String suffix) {
    checkArgument(symbolMap.containsKey(symbolHint), "symbolHint not in symbols map");
    return newSymbol(symbolHint.getName(), symbolMap.get(symbolHint), suffix);
  }

  public Symbol newSymbol(String symbolHint, Type type) {
    return newSymbol(symbolHint, type, null);
  }

  public Symbol newSymbol(String symbolHint, Type symbolType, String suffix) {
    requireNonNull(symbolHint, "symbolHint is null");
    requireNonNull(symbolType, "type is null");

    if (suffix != null) {
      symbolHint = symbolHint + SEPARATOR + suffix;
    }

    Symbol symbol = new Symbol(symbolHint);
    while (symbolMap.putIfAbsent(symbol, symbolType) != null) {
      symbol = new Symbol(symbolHint + "_" + nextId());
    }

    return symbol;
  }

  public Symbol newSymbol(Expression expression, Type type) {
    return newSymbol(expression, type, null);
  }

  public Symbol newSymbol(Expression expression, Type type, String suffix) {
    String nameHint = "expr";
    if (expression instanceof Identifier) {
      nameHint = ((Identifier) expression).getValue();
    } else if (expression instanceof FunctionCall) {
      nameHint = ((FunctionCall) expression).getName().getSuffix();
    } else if (expression instanceof SymbolReference) {
      nameHint = ((SymbolReference) expression).getName();
    }
    /*else if (expression instanceof GroupingOperation) {
      nameHint = "grouping";
    }*/

    return newSymbol(nameHint, type, suffix);
  }

  public Symbol newSymbol(Field field) {
    String symbolHint = field.getName().orElse("field");
    return newSymbol(symbolHint, field.getType());
  }

  public TypeProvider getTypes() {
    return TypeProvider.viewOf(symbolMap);
  }

  private int nextId() {
    return nextId++;
  }
}
