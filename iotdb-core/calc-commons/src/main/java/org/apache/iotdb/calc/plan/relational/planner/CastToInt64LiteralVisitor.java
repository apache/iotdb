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

package org.apache.iotdb.calc.plan.relational.planner;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FloatLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;

// return NULL, if we cannot parse literal to INT64 type
public class CastToInt64LiteralVisitor implements CommonQueryAstVisitor<Long, Void> {

  @Override
  public Long visitLiteral(Literal node, Void context) {
    throw new UnsupportedOperationException("Unhandled literal type: " + node);
  }

  @Override
  public Long visitBooleanLiteral(BooleanLiteral node, Void context) {
    return node.getValue() ? 1L : 0L;
  }

  @Override
  public Long visitLongLiteral(LongLiteral node, Void context) {
    return node.getParsedValue();
  }

  @Override
  public Long visitDoubleLiteral(DoubleLiteral node, Void context) {
    return (long) node.getValue();
  }

  @Override
  public Long visitFloatLiteral(FloatLiteral node, Void context) {
    return (long) node.getValue();
  }

  @Override
  public Long visitStringLiteral(StringLiteral node, Void context) {
    try {
      return Long.parseLong(node.getValue());
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public Long visitBinaryLiteral(BinaryLiteral node, Void context) {
    return null;
  }

  @Override
  public Long visitGenericLiteral(GenericLiteral node, Void context) {
    try {
      return Long.parseLong(node.getValue());
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public Long visitNullLiteral(NullLiteral node, Void context) {
    return null;
  }
}
