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

import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;

// return NULL, if we cannot parse literal to double type
public class CastToDoubleLiteralVisitor extends AstVisitor<Double, Void> {

  @Override
  protected Double visitLiteral(Literal node, Void context) {
    throw new UnsupportedOperationException("Unhandled literal type: " + node);
  }

  @Override
  protected Double visitBooleanLiteral(BooleanLiteral node, Void context) {
    return node.getValue() ? 1.0d : 0.0d;
  }

  @Override
  protected Double visitLongLiteral(LongLiteral node, Void context) {
    return (double) node.getParsedValue();
  }

  @Override
  protected Double visitDoubleLiteral(DoubleLiteral node, Void context) {
    return node.getValue();
  }

  @Override
  protected Double visitStringLiteral(StringLiteral node, Void context) {
    try {
      return Double.parseDouble(node.getValue());
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  protected Double visitBinaryLiteral(BinaryLiteral node, Void context) {
    return null;
  }

  @Override
  protected Double visitGenericLiteral(GenericLiteral node, Void context) {
    try {
      return Double.parseDouble(node.getValue());
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  protected Double visitNullLiteral(NullLiteral node, Void context) {
    return null;
  }
}
