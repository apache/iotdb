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

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import java.nio.charset.Charset;

// return NULL, if we cannot parse literal to string/text type
public class CastToStringLiteralVisitor extends AstVisitor<Binary, Void> {

  private final Charset charset;

  public CastToStringLiteralVisitor(Charset charset) {
    this.charset = charset;
  }

  @Override
  protected Binary visitLiteral(Literal node, Void context) {
    throw new UnsupportedOperationException("Unhandled literal type: " + node);
  }

  @Override
  protected Binary visitBooleanLiteral(BooleanLiteral node, Void context) {
    return new Binary(String.valueOf(node.getValue()), charset);
  }

  @Override
  protected Binary visitLongLiteral(LongLiteral node, Void context) {
    return new Binary(node.getValue(), charset);
  }

  @Override
  protected Binary visitDoubleLiteral(DoubleLiteral node, Void context) {
    return new Binary(String.valueOf(node.getValue()), charset);
  }

  @Override
  protected Binary visitStringLiteral(StringLiteral node, Void context) {
    return new Binary(node.getValue(), charset);
  }

  @Override
  protected Binary visitBinaryLiteral(BinaryLiteral node, Void context) {
    return new Binary(BytesUtils.parseBlobByteArrayToString(node.getValue()), charset);
  }

  @Override
  protected Binary visitGenericLiteral(GenericLiteral node, Void context) {
    try {
      return new Binary(node.getValue(), charset);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  protected Binary visitNullLiteral(NullLiteral node, Void context) {
    return null;
  }
}
