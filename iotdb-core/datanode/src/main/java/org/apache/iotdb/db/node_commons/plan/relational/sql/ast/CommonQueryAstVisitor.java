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

package org.apache.iotdb.db.node_commons.plan.relational.sql.ast;

import javax.annotation.Nullable;

public abstract class CommonQueryAstVisitor<R, C> implements IAstVisitor<R, C> {

  public R process(Node node) {
    return process(node, null);
  }

  public R process(Node node, @Nullable C context) {
    return node.accept(this, context);
  }

  protected R visitNode(Node node, C context) {
    return null;
  }

  protected R visitExpression(Expression node, C context) {
    return visitNode(node, context);
  }

  protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitBetweenPredicate(BetweenPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitCast(Cast node, C context) {
    return visitExpression(node, context);
  }

  protected R visitExtract(Extract node, C context) {
    return visitExpression(node, context);
  }

  protected R visitBooleanLiteral(BooleanLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitLiteral(Literal node, C context) {
    return visitExpression(node, context);
  }

  protected R visitBinaryLiteral(BinaryLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitStringLiteral(StringLiteral node, C context) {
    return visitLiteral(node, context);
  }
}
