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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.DateType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import static java.util.Objects.requireNonNull;

public class LiteralInterpreter {

  private final PlannerContext plannerContext;
  private final SessionInfo session;

  public LiteralInterpreter(PlannerContext plannerContext, SessionInfo session) {
    this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    this.session = session;
  }

  public Object evaluate(Expression node, Type type) {
    if (!(node instanceof Literal)) {
      throw new IllegalArgumentException("node must be a Literal");
    }
    return new LiteralVisitor(type).process(node, null);
  }

  private class LiteralVisitor extends AstVisitor<Object, Void> {
    private final Type type;

    private LiteralVisitor(Type type) {
      this.type = requireNonNull(type, "type is null");
    }

    @Override
    protected Object visitLiteral(Literal node, Void context) {
      throw new UnsupportedOperationException("Unhandled literal type: " + node);
    }

    @Override
    protected Boolean visitBooleanLiteral(BooleanLiteral node, Void context) {
      return node.getValue();
    }

    @Override
    protected Long visitLongLiteral(LongLiteral node, Void context) {
      return node.getParsedValue();
    }

    @Override
    protected Double visitDoubleLiteral(DoubleLiteral node, Void context) {
      return node.getValue();
    }

    @Override
    protected Binary visitStringLiteral(StringLiteral node, Void context) {
      return new Binary(node.getValue(), TSFileConfig.STRING_CHARSET);
    }

    @Override
    protected Binary visitBinaryLiteral(BinaryLiteral node, Void context) {
      return new Binary(node.getValue());
    }

    @Override
    protected Object visitGenericLiteral(GenericLiteral node, Void context) {
      if (type.equals(TimestampType.TIMESTAMP)) {
        return Long.parseLong(node.getValue());
      } else if (type.equals(DateType.DATE)) {
        return Integer.parseInt(node.getValue());
      } else {
        throw new SemanticException(String.format("No literal form for type %s", type));
      }
    }

    @Override
    protected Object visitNullLiteral(NullLiteral node, Void context) {
      return null;
    }
  }
}
