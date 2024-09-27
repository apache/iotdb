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
import org.apache.iotdb.db.utils.DateTimeUtils;

import java.time.ZoneId;

// return NULL, if we cannot parse literal to timestamp type
public class CastToTimestampLiteralVisitor extends AstVisitor<Long, Void> {

  private final ZoneId zoneId;

  public CastToTimestampLiteralVisitor(ZoneId zoneId) {
    this.zoneId = zoneId;
  }

  @Override
  protected Long visitLiteral(Literal node, Void context) {
    throw new UnsupportedOperationException("Unhandled literal type: " + node);
  }

  @Override
  protected Long visitBooleanLiteral(BooleanLiteral node, Void context) {
    return null;
  }

  @Override
  protected Long visitLongLiteral(LongLiteral node, Void context) {
    return node.getParsedValue();
  }

  @Override
  protected Long visitDoubleLiteral(DoubleLiteral node, Void context) {
    return (long) node.getValue();
  }

  @Override
  protected Long visitStringLiteral(StringLiteral node, Void context) {
    try {
      return DateTimeUtils.convertDatetimeStrToLong(node.getValue(), zoneId);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  protected Long visitBinaryLiteral(BinaryLiteral node, Void context) {
    return null;
  }

  @Override
  protected Long visitGenericLiteral(GenericLiteral node, Void context) {
    try {
      return DateTimeUtils.convertDatetimeStrToLong(node.getValue(), zoneId);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  protected Long visitNullLiteral(NullLiteral node, Void context) {
    return null;
  }
}
