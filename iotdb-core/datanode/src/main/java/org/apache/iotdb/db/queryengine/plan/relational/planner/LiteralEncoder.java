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

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FloatLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.type.DateType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.UnknownType;
import org.apache.tsfile.utils.Binary;

import javax.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils.isBlobType;
import static org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils.isBool;
import static org.apache.iotdb.calc.plan.relational.metadata.CommonMetadataUtils.isCharType;
import static org.apache.iotdb.commons.queryengine.plan.relational.type.TypeSignatureTranslator.toSqlType;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.FloatType.FLOAT;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;

public final class LiteralEncoder {

  private final PlannerContext plannerContext;

  public LiteralEncoder(PlannerContext plannerContext) {
    this.plannerContext =
        requireNonNull(
            plannerContext, DataNodeQueryMessages.EXCEPTION_PLANNERCONTEXT_IS_NULL_B7C7DE50);
  }

  public List<Expression> toExpressions(List<?> objects, List<? extends Type> types) {
    requireNonNull(objects, DataNodeQueryMessages.EXCEPTION_OBJECTS_IS_NULL_819EE879);
    requireNonNull(types, DataNodeQueryMessages.EXCEPTION_TYPES_IS_NULL_E4B2309D);
    checkArgument(
        objects.size() == types.size(),
        DataNodeQueryMessages.EXCEPTION_OBJECTS_AND_TYPES_DO_NOT_HAVE_THE_SAME_SIZE_8B51E17B);

    ImmutableList.Builder<Expression> expressions = ImmutableList.builder();
    for (int i = 0; i < objects.size(); i++) {
      Object object = objects.get(i);
      Type type = types.get(i);
      expressions.add(toExpression(object, type));
    }
    return expressions.build();
  }

  public Expression toExpression(@Nullable Object object, Type type) {
    requireNonNull(type, DataNodeQueryMessages.EXCEPTION_TYPE_IS_NULL_16A3D3EB);

    if (object instanceof Expression) {
      return (Expression) object;
    }

    if (object == null) {
      if (type.equals(UnknownType.UNKNOWN)) {
        return new NullLiteral();
      }
      return new Cast(new NullLiteral(), toSqlType(type), false);
    }

    if (type.equals(INT32) || type.equals(INT64)) {
      return new LongLiteral(object.toString());
    }

    if (type.equals(DOUBLE)) {
      return new DoubleLiteral(((Number) object).doubleValue());
    }

    if (type.equals(FLOAT)) {
      return new FloatLiteral(((Number) object).floatValue());
    }

    if (isBool(type)) {
      return new BooleanLiteral(object.toString());
    }

    if (isCharType(type)) {
      Binary value = (Binary) object;
      return new StringLiteral(value.getStringValue(TSFileConfig.STRING_CHARSET));
    }

    if (isBlobType(type)) {
      Binary value = (Binary) object;
      return new BinaryLiteral(value.getValues());
    }

    if (type.equals(DateType.DATE)) {
      return new GenericLiteral(DateType.DATE.getTypeEnum().name(), ((Integer) object).toString());
    }

    if (type.equals(TimestampType.TIMESTAMP)) {
      return new GenericLiteral(TimestampType.TIMESTAMP.getTypeEnum().name(), object.toString());
    }

    throw new IllegalArgumentException(DataNodeQueryMessages.UNKNOWN_TYPE_2 + type);
  }
}
