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
package org.apache.iotdb.db.queryengine.plan.relational.planner.ir;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.ColumnSchema;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;

import java.util.Map;

public class ReplaceSymbolInExpression {

  private ReplaceSymbolInExpression() {}

  public static Expression transform(
      Expression expression, Map<Symbol, ColumnSchema> tableAssignments) {
    return ExpressionTreeRewriter.rewriteWith(
        new Visitor(), expression, new Context(tableAssignments));
  }

  private static class Visitor extends ExpressionRewriter<Context> {

    @Override
    public Expression rewriteSymbolReference(
        SymbolReference node, Context context, ExpressionTreeRewriter<Context> treeRewriter) {
      return new SymbolReference(context.tableAssignments.get(Symbol.of(node.getName())).getName());
    }
  }

  public static class Context {
    Map<Symbol, ColumnSchema> tableAssignments;

    public Context(Map<Symbol, ColumnSchema> tableAssignments) {
      this.tableAssignments = tableAssignments;
    }
  }
}
