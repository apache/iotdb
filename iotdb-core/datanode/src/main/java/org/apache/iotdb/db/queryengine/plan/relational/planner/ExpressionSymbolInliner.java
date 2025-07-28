/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.planner.ir.ExpressionTreeRewriter;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

public final class ExpressionSymbolInliner {
  public static Expression inlineSymbols(
      Map<Symbol, ? extends Expression> mapping, Expression expression) {
    return inlineSymbols(mapping::get, expression);
  }

  public static Expression inlineSymbols(
      Function<Symbol, Expression> mapping, Expression expression) {
    return new ExpressionSymbolInliner(mapping).rewrite(expression);
  }

  private final Function<Symbol, Expression> mapping;

  private ExpressionSymbolInliner(Function<Symbol, Expression> mapping) {
    this.mapping = mapping;
  }

  private Expression rewrite(Expression expression) {
    return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
  }

  private class Visitor extends ExpressionRewriter<Void> {
    private final Multiset<String> excludedNames = HashMultiset.create();

    @Override
    public Expression rewriteSymbolReference(
        SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter) {
      if (excludedNames.contains(node.getName())) {
        return node;
      }

      Expression expression = mapping.apply(Symbol.from(node));
      checkArgument(expression != null, "Cannot resolve symbol %s", node.getName());
      return expression;
    }
  }
}
