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

import org.apache.tsfile.read.common.type.Type;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class SymbolAllocator {
  private final Map<Symbol, Type> symbolMap;
  private int nextId;

  public SymbolAllocator() {
    symbolMap = new HashMap<>();
  }

  public Symbol newSymbol(String symbolHint, Type type) {
    return newSymbol(symbolHint, type, null);
  }

  public Symbol newSymbol(String symbolHint, Type symbolType, String suffix) {
    requireNonNull(symbolHint, "symbolHint is null");
    requireNonNull(symbolType, "type is null");

    if (suffix != null) {
      symbolHint = symbolHint + "$" + suffix;
    }

    Symbol symbol = new Symbol(symbolHint);
    while (symbolMap.putIfAbsent(symbol, symbolType) != null) {
      symbol = new Symbol(symbolHint + "_" + nextId());
    }

    return symbol;
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
