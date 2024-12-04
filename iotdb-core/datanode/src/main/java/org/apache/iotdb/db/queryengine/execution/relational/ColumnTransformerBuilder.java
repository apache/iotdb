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

package org.apache.iotdb.db.queryengine.execution.relational;

import org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction;
import org.apache.iotdb.commons.udf.utils.TableUDFUtils;
import org.apache.iotdb.commons.udf.utils.UDFDataTypeTransformer;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.AdditionResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.DivisionResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.ModulusResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.MultiplicationResolver;
import org.apache.iotdb.db.queryengine.plan.relational.function.arithmetic.SubtractionResolver;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.AstVisitor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentDatabase;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentTime;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CurrentUser;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DecimalLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.WhenClause;
import org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.TableCaseWhenThenColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticColumnTransformerApi;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareEqualToColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareGreaterEqualColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareGreaterThanColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareLessEqualColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareLessThanColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareNonEqualColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.Like2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.NullColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.TimeColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.CoalesceColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.InBinaryMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.InBooleanMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.InDoubleMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.InFloatMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.InInt32MultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.InInt64MultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.InMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.LogicalAndMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.LogicalOrMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ternary.BetweenColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ternary.Like3ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.udf.UserDefineScalarFunctionTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.IsNullColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.LikeColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.LogicNotColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.AbsColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.AcosColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.AsinColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.AtanColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.CastFunctionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.CeilColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.Concat2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.ConcatColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.ConcatMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.CosColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.CoshColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DateBinFunctionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DegreesColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DiffColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.DiffFunctionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.EndsWith2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.EndsWithColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.ExpColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.FloorColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.LTrim2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.LTrimColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.LengthColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.LnColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.Log10ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.LowerColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.RTrim2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.RTrimColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.RadiansColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.RegexpLike2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.RegexpLikeColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.Replace2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.Replace3ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.ReplaceFunctionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.RoundColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.RoundFunctionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.SignColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.SinColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.SinhColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.SqrtColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.StartsWith2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.StartsWithColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.Strcmp2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.StrcmpColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.Strpos2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.StrposColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.SubString2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.SubString3ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.SubStringColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TanColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TanhColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.Trim2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TrimColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TryCastFunctionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.UpperColumnTransformer;
import org.apache.iotdb.udf.api.customizer.config.ScalarFunctionConfig;
import org.apache.iotdb.udf.api.customizer.parameter.FunctionParameters;
import org.apache.iotdb.udf.api.relational.ScalarFunction;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.common.regexp.LikePattern;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.DateType;
import org.apache.tsfile.read.common.type.TimestampType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.read.common.type.TypeEnum;
import org.apache.tsfile.utils.Binary;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.queryengine.plan.expression.unary.LikeExpression.getEscapeCharacter;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoMetadataChecker.isStringLiteral;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toTypeSignature;
import static org.apache.tsfile.read.common.type.BlobType.BLOB;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;

public class ColumnTransformerBuilder
    extends AstVisitor<ColumnTransformer, ColumnTransformerBuilder.Context> {

  private static final String UNSUPPORTED_EXPRESSION = "Unsupported expression: %s";

  @Override
  public ColumnTransformer visitExpression(Expression expression, Context context) {
    throw new IllegalArgumentException(
        String.format(UNSUPPORTED_EXPRESSION, expression.getClass().getSimpleName()));
  }

  @Override
  protected ColumnTransformer visitArithmeticBinary(
      ArithmeticBinaryExpression node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        ColumnTransformer left = process(node.getLeft(), context);
        ColumnTransformer right = process(node.getRight(), context);
        List<Type> types = Arrays.asList(left.getType(), right.getType());
        Type type;
        switch (node.getOperator()) {
          case ADD:
            type = AdditionResolver.checkConditions(types).get();
            break;
          case SUBTRACT:
            type = SubtractionResolver.checkConditions(types).get();
            break;
          case MULTIPLY:
            type = MultiplicationResolver.checkConditions(types).get();
            break;
          case DIVIDE:
            type = DivisionResolver.checkConditions(types).get();
            break;
          case MODULUS:
            type = ModulusResolver.checkConditions(types).get();
            break;
          default:
            throw new UnsupportedOperationException(
                String.format(UNSUPPORTED_EXPRESSION, node.getOperator()));
        }
        TSDataType tsDataType = InternalTypeManager.getTSDataType(type);
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                type, context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(tsDataType);
        context.cache.put(node, identity);
      } else {
        ZoneId zoneId = context.sessionInfo.getZoneId();
        ColumnTransformer left = process(node.getLeft(), context);
        ColumnTransformer right = process(node.getRight(), context);
        ColumnTransformer child;
        switch (node.getOperator()) {
          case ADD:
            child = ArithmeticColumnTransformerApi.getAdditionTransformer(left, right, zoneId);
            break;
          case SUBTRACT:
            child = ArithmeticColumnTransformerApi.getSubtractionTransformer(left, right, zoneId);
            break;
          case MULTIPLY:
            child =
                ArithmeticColumnTransformerApi.getMultiplicationTransformer(left, right, zoneId);
            break;
          case DIVIDE:
            child = ArithmeticColumnTransformerApi.getDivisionTransformer(left, right, zoneId);
            break;
          case MODULUS:
            child = ArithmeticColumnTransformerApi.getModulusTransformer(left, right, zoneId);
            break;
          default:
            throw new UnsupportedOperationException(
                String.format(UNSUPPORTED_EXPRESSION, node.getOperator()));
        }
        context.cache.put(node, child);
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitArithmeticUnary(
      ArithmeticUnaryExpression node, Context context) {
    switch (node.getSign()) {
      case PLUS:
        return process(node.getValue(), context);
      case MINUS:
        if (!context.cache.containsKey(node)) {
          if (context.hasSeen.containsKey(node)) {
            IdentityColumnTransformer identity =
                new IdentityColumnTransformer(
                    DOUBLE, context.originSize + context.commonTransformerList.size());
            ColumnTransformer columnTransformer = context.hasSeen.get(node);
            columnTransformer.addReferenceCount();
            context.commonTransformerList.add(columnTransformer);
            context.leafList.add(identity);
            context.inputDataTypes.add(TSDataType.DOUBLE);
            context.cache.put(node, identity);
          } else {
            ColumnTransformer childColumnTransformer = process(node.getValue(), context);
            context.cache.put(
                node,
                ArithmeticColumnTransformerApi.getNegationTransformer(childColumnTransformer));
          }
        }
        ColumnTransformer res = context.cache.get(node);
        res.addReferenceCount();
        return res;
      default:
        throw new UnsupportedOperationException("Unknown sign: " + node.getSign());
    }
  }

  @Override
  protected ColumnTransformer visitBetweenPredicate(BetweenPredicate node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                BOOLEAN, context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(TSDataType.BOOLEAN);
        context.cache.put(node, identity);
      } else {
        ColumnTransformer value = this.process(node.getValue(), context);
        ColumnTransformer min = this.process(node.getMin(), context);
        ColumnTransformer max = this.process(node.getMax(), context);
        context.cache.put(node, new BetweenColumnTransformer(BOOLEAN, value, min, max, false));
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitCast(Cast node, Context context) {

    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                columnTransformer.getType(),
                context.originSize + context.commonTransformerList.size());
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(getTSDataType(columnTransformer.getType()));
        context.cache.put(node, identity);
      } else {
        ColumnTransformer child = this.process(node.getExpression(), context);
        Type type;
        try {
          type = context.metadata.getType(toTypeSignature(node.getType()));
        } catch (TypeNotFoundException e) {
          throw new SemanticException(String.format("Unknown type: %s", node.getType()));
        }
        context.cache.put(
            node,
            node.isSafe()
                ? new TryCastFunctionColumnTransformer(type, child, context.sessionInfo.getZoneId())
                : new CastFunctionColumnTransformer(type, child, context.sessionInfo.getZoneId()));
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitBooleanLiteral(BooleanLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      BOOLEAN,
                      new BooleanColumn(1, Optional.empty(), new boolean[] {node.getValue()}));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitBinaryLiteral(BinaryLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      BLOB,
                      new BinaryColumn(
                          1, Optional.empty(), new Binary[] {new Binary(node.getValue())}));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitStringLiteral(StringLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      STRING,
                      new BinaryColumn(
                          1,
                          Optional.empty(),
                          new Binary[] {new Binary(node.getValue(), TSFileConfig.STRING_CHARSET)}));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitLongLiteral(LongLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer;
              if (node.getParsedValue() >= Integer.MIN_VALUE
                  && node.getParsedValue() <= Integer.MAX_VALUE) {
                columnTransformer =
                    new ConstantColumnTransformer(
                        INT32,
                        new IntColumn(
                            1, Optional.empty(), new int[] {(int) node.getParsedValue()}));
              } else {
                columnTransformer =
                    new ConstantColumnTransformer(
                        INT64,
                        new LongColumn(1, Optional.empty(), new long[] {node.getParsedValue()}));
              }
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitDoubleLiteral(DoubleLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      DOUBLE,
                      new DoubleColumn(1, Optional.empty(), new double[] {node.getValue()}));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitDecimalLiteral(DecimalLiteral node, Context context) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected ColumnTransformer visitGenericLiteral(GenericLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  getColumnTransformerForGenericLiteral(node);
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  // currently, we only support Date and Timestamp
  // for Date, GenericLiteral.value is an int value
  // for Timestamp, GenericLiteral.value is a long value
  private static ConstantColumnTransformer getColumnTransformerForGenericLiteral(
      GenericLiteral literal) {
    if (DateType.DATE.getTypeEnum().name().equals(literal.getType())) {
      return new ConstantColumnTransformer(
          DateType.DATE,
          new IntColumn(1, Optional.empty(), new int[] {Integer.parseInt(literal.getValue())}));
    } else if (TimestampType.TIMESTAMP.getTypeEnum().name().equals(literal.getType())) {
      return new ConstantColumnTransformer(
          TimestampType.TIMESTAMP,
          new LongColumn(1, Optional.empty(), new long[] {Long.parseLong(literal.getValue())}));
    } else {
      throw new SemanticException("Unsupported type in GenericLiteral: " + literal.getType());
    }
  }

  @Override
  protected ColumnTransformer visitNullLiteral(NullLiteral node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              NullColumnTransformer columnTransformer = new NullColumnTransformer();
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitComparisonExpression(
      ComparisonExpression node, Context context) {
    ColumnTransformer comparisonTransformer;
    if (context.cache.containsKey(node)) {
      comparisonTransformer = context.cache.get(node);
    } else {
      comparisonTransformer = getColumnTransformer(node, context);
      context.cache.put(node, comparisonTransformer);
    }
    comparisonTransformer.addReferenceCount();
    return comparisonTransformer;
  }

  private ColumnTransformer getColumnTransformer(ComparisonExpression node, Context context) {
    if (context.hasSeen.containsKey(node)) {
      IdentityColumnTransformer identity =
          new IdentityColumnTransformer(
              BOOLEAN, context.originSize + context.commonTransformerList.size());
      ColumnTransformer columnTransformer = context.hasSeen.get(node);
      columnTransformer.addReferenceCount();
      context.commonTransformerList.add(columnTransformer);
      context.leafList.add(identity);
      context.inputDataTypes.add(TSDataType.BOOLEAN);
      return identity;
    } else {
      ColumnTransformer left = process(node.getLeft(), context);
      ColumnTransformer right = process(node.getRight(), context);
      switch (node.getOperator()) {
        case EQUAL:
          return new CompareEqualToColumnTransformer(BOOLEAN, left, right);
        case NOT_EQUAL:
          return new CompareNonEqualColumnTransformer(BOOLEAN, left, right);
        case GREATER_THAN:
          return new CompareGreaterThanColumnTransformer(BOOLEAN, left, right);
        case GREATER_THAN_OR_EQUAL:
          return new CompareGreaterEqualColumnTransformer(BOOLEAN, left, right);
        case LESS_THAN:
          return new CompareLessThanColumnTransformer(BOOLEAN, left, right);
        case LESS_THAN_OR_EQUAL:
          return new CompareLessEqualColumnTransformer(BOOLEAN, left, right);
        default:
          throw new UnsupportedOperationException(
              String.format(UNSUPPORTED_EXPRESSION, node.getOperator()));
      }
    }
  }

  @Override
  protected ColumnTransformer visitCurrentDatabase(CurrentDatabase node, Context context) {
    Optional<String> currentDatabase = context.sessionInfo.getDatabaseName();
    ColumnTransformer res;
    res =
        currentDatabase
            .map(
                s ->
                    context.cache.computeIfAbsent(
                        node,
                        e -> {
                          ConstantColumnTransformer columnTransformer =
                              new ConstantColumnTransformer(
                                  STRING,
                                  new BinaryColumn(
                                      1,
                                      Optional.empty(),
                                      new Binary[] {new Binary(s, TSFileConfig.STRING_CHARSET)}));
                          context.leafList.add(columnTransformer);
                          return columnTransformer;
                        }))
            .orElseGet(
                () ->
                    context.cache.computeIfAbsent(
                        node,
                        e -> {
                          NullColumnTransformer columnTransformer = new NullColumnTransformer();
                          context.leafList.add(columnTransformer);
                          return columnTransformer;
                        }));
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitCurrentTime(CurrentTime node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitCurrentUser(CurrentUser node, Context context) {
    String currentUser = context.sessionInfo.getUserName();
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              ConstantColumnTransformer columnTransformer =
                  new ConstantColumnTransformer(
                      STRING,
                      new BinaryColumn(
                          1,
                          Optional.empty(),
                          new Binary[] {new Binary(currentUser, TSFileConfig.STRING_CHARSET)}));
              context.leafList.add(columnTransformer);
              return columnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitFunctionCall(FunctionCall node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                columnTransformer.getType(),
                context.originSize + context.commonTransformerList.size());
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(getTSDataType(columnTransformer.getType()));
        context.cache.put(node, identity);
      } else {
        context.cache.put(
            node,
            getFunctionColumnTransformer(node.getName().getSuffix(), node.getArguments(), context));
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  private ColumnTransformer getFunctionColumnTransformer(
      String functionName, List<Expression> children, Context context) {
    // builtin scalar function
    if (TableBuiltinScalarFunction.DIFF.getFunctionName().equalsIgnoreCase(functionName)) {
      boolean ignoreNull = true;
      if (children.size() > 1) {
        if (isBooleanLiteral(children.get(1))) {
          ignoreNull = ((BooleanLiteral) children.get(1)).getValue();
        } else {
          return new DiffColumnTransformer(
              DOUBLE,
              this.process(children.get(0), context),
              this.process(children.get(1), context));
        }
      }
      return new DiffFunctionColumnTransformer(
          DOUBLE, this.process(children.get(0), context), ignoreNull);
    } else if (TableBuiltinScalarFunction.ROUND.getFunctionName().equalsIgnoreCase(functionName)) {
      int places = 0;
      if (children.size() > 1) {
        if (isLongLiteral(children.get(1))) {
          places = (int) ((LongLiteral) children.get(1)).getParsedValue();
        } else {
          return new RoundColumnTransformer(
              DOUBLE,
              this.process(children.get(0), context),
              this.process(children.get(1), context));
        }
      }
      return new RoundFunctionColumnTransformer(
          DOUBLE, this.process(children.get(0), context), places);
    } else if (TableBuiltinScalarFunction.REPLACE
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 2) {
        if (isStringLiteral(children.get(1))) {
          return new ReplaceFunctionColumnTransformer(
              STRING, first, ((StringLiteral) children.get(1)).getValue(), "");
        } else {
          return new Replace2ColumnTransformer(
              STRING, first, this.process(children.get(1), context));
        }
      } else {
        // size == 3
        if (isStringLiteral(children.get(1)) && isStringLiteral(children.get(2))) {
          return new ReplaceFunctionColumnTransformer(
              STRING,
              first,
              ((StringLiteral) children.get(1)).getValue(),
              ((StringLiteral) children.get(2)).getValue());
        } else {
          return new Replace3ColumnTransformer(
              STRING,
              first,
              this.process(children.get(1), context),
              this.process(children.get(2), context));
        }
      }
    } else if (TableBuiltinScalarFunction.SUBSTRING
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 2) {
        if (isLongLiteral(children.get(1))) {
          int startIndex = (int) ((LongLiteral) children.get(1)).getParsedValue();
          return new SubStringColumnTransformer(STRING, first, startIndex, Integer.MAX_VALUE);
        } else {
          return new SubString2ColumnTransformer(
              STRING, first, this.process(children.get(1), context));
        }
      } else {
        // size == 3
        if (isLongLiteral(children.get(1)) && isLongLiteral(children.get(2))) {
          int startIndex = (int) ((LongLiteral) children.get(1)).getParsedValue();
          int length = (int) ((LongLiteral) children.get(2)).getParsedValue();
          return new SubStringColumnTransformer(STRING, first, startIndex, length);
        } else {
          return new SubString3ColumnTransformer(
              STRING,
              first,
              this.process(children.get(1), context),
              this.process(children.get(2), context));
        }
      }
    } else if (TableBuiltinScalarFunction.LENGTH.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new LengthColumnTransformer(INT32, first);
      }
    } else if (TableBuiltinScalarFunction.UPPER.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new UpperColumnTransformer(STRING, first);
      }
    } else if (TableBuiltinScalarFunction.LOWER.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new LowerColumnTransformer(STRING, first);
      }
    } else if (TableBuiltinScalarFunction.TRIM.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new TrimColumnTransformer(STRING, first, " ");
      } else {
        // children.size() == 2
        if (isStringLiteral(children.get(1))) {
          return new TrimColumnTransformer(
              STRING, first, ((StringLiteral) children.get(1)).getValue());
        } else {
          return new Trim2ColumnTransformer(STRING, first, this.process(children.get(1), context));
        }
      }
    } else if (TableBuiltinScalarFunction.LTRIM.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new LTrimColumnTransformer(STRING, first, " ");
      } else {
        // children.size() == 2
        if (isStringLiteral(children.get(1))) {
          return new LTrimColumnTransformer(
              STRING, first, ((StringLiteral) children.get(1)).getValue());
        } else {
          return new LTrim2ColumnTransformer(STRING, first, this.process(children.get(1), context));
        }
      }
    } else if (TableBuiltinScalarFunction.RTRIM.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new RTrimColumnTransformer(STRING, first, " ");
      } else {
        // children.size() == 2
        if (isStringLiteral(children.get(1))) {
          return new RTrimColumnTransformer(
              STRING, first, ((StringLiteral) children.get(1)).getValue());
        } else {
          return new RTrim2ColumnTransformer(STRING, first, this.process(children.get(1), context));
        }
      }
    } else if (TableBuiltinScalarFunction.REGEXP_LIKE
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 2) {
        if (isStringLiteral(children.get(1))) {
          return new RegexpLikeColumnTransformer(
              BOOLEAN, first, ((StringLiteral) children.get(1)).getValue());
        } else {
          return new RegexpLike2ColumnTransformer(
              BOOLEAN, first, this.process(children.get(1), context));
        }
      }
    } else if (TableBuiltinScalarFunction.STRPOS.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 2) {
        if (isStringLiteral(children.get(1))) {
          return new StrposColumnTransformer(
              INT32, first, ((StringLiteral) children.get(1)).getValue());
        } else {
          return new Strpos2ColumnTransformer(INT32, first, this.process(children.get(1), context));
        }
      }
    } else if (TableBuiltinScalarFunction.STARTS_WITH
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 2) {
        if (isStringLiteral(children.get(1))) {
          return new StartsWithColumnTransformer(
              BOOLEAN, first, ((StringLiteral) children.get(1)).getValue());
        } else {
          return new StartsWith2ColumnTransformer(
              BOOLEAN, first, this.process(children.get(1), context));
        }
      }
    } else if (TableBuiltinScalarFunction.ENDS_WITH
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 2) {
        if (isStringLiteral(children.get(1))) {
          return new EndsWithColumnTransformer(
              BOOLEAN, first, ((StringLiteral) children.get(1)).getValue());
        } else {
          return new EndsWith2ColumnTransformer(
              BOOLEAN, first, this.process(children.get(1), context));
        }
      }
    } else if (TableBuiltinScalarFunction.CONCAT.getFunctionName().equalsIgnoreCase(functionName)) {
      if (children.size() == 2) {
        if (isStringLiteral(children.get(1)) && !isStringLiteral(children.get(0))) {
          return new ConcatColumnTransformer(
              STRING,
              this.process(children.get(0), context),
              ((StringLiteral) children.get(1)).getValue(),
              true);
        } else if (isStringLiteral(children.get(0)) && !isStringLiteral(children.get(1))) {
          return new ConcatColumnTransformer(
              STRING,
              this.process(children.get(1), context),
              ((StringLiteral) children.get(0)).getValue(),
              false);
        } else {
          return new Concat2ColumnTransformer(
              STRING,
              this.process(children.get(0), context),
              this.process(children.get(1), context));
        }
      } else {
        List<ColumnTransformer> columnTransformers = new ArrayList<>();
        for (Expression child : children) {
          columnTransformers.add(this.process(child, context));
        }
        return new ConcatMultiColumnTransformer(STRING, columnTransformers);
      }
    } else if (TableBuiltinScalarFunction.STRCMP.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 2) {
        if (isStringLiteral(children.get(1))) {
          return new StrcmpColumnTransformer(
              INT32, first, ((StringLiteral) children.get(1)).getValue());
        } else {
          return new Strcmp2ColumnTransformer(INT32, first, this.process(children.get(1), context));
        }
      }
    } else if (TableBuiltinScalarFunction.SIN.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new SinColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.COS.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new CosColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.TAN.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new TanColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.ASIN.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new AsinColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.ACOS.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new AcosColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.ATAN.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new AtanColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.SINH.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new SinhColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.COSH.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new CoshColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.TANH.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new TanhColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.DEGREES
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new DegreesColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.RADIANS
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new RadiansColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.ABS.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new AbsColumnTransformer(first.getType(), first);
      }
    } else if (TableBuiltinScalarFunction.SIGN.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new SignColumnTransformer(first.getType(), first);
      }
    } else if (TableBuiltinScalarFunction.CEIL.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new CeilColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.FLOOR.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new FloorColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.EXP.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new ExpColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.LN.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new LnColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.LOG10.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new Log10ColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.SQRT.getFunctionName().equalsIgnoreCase(functionName)) {
      ColumnTransformer first = this.process(children.get(0), context);
      if (children.size() == 1) {
        return new SqrtColumnTransformer(DOUBLE, first);
      }
    } else if (TableBuiltinScalarFunction.PI.getFunctionName().equalsIgnoreCase(functionName)) {
      ConstantColumnTransformer piColumnTransformer =
          new ConstantColumnTransformer(
              DOUBLE, new DoubleColumn(1, Optional.empty(), new double[] {Math.PI}));
      context.leafList.add(piColumnTransformer);
      return piColumnTransformer;
    } else if (TableBuiltinScalarFunction.E.getFunctionName().equalsIgnoreCase(functionName)) {
      ConstantColumnTransformer eColumnTransformer =
          new ConstantColumnTransformer(
              DOUBLE, new DoubleColumn(1, Optional.empty(), new double[] {Math.E}));
      context.leafList.add(eColumnTransformer);
      return eColumnTransformer;
    } else if (TableBuiltinScalarFunction.DATE_BIN
        .getFunctionName()
        .equalsIgnoreCase(functionName)) {
      ColumnTransformer source = this.process(children.get(2), context);
      return new DateBinFunctionColumnTransformer(
          source.getType(),
          ((LongLiteral) children.get(0)).getParsedValue(),
          ((LongLiteral) children.get(1)).getParsedValue(),
          source,
          ((LongLiteral) children.get(3)).getParsedValue(),
          context.sessionInfo.getZoneId());
    } else {
      // user defined function
      if (TableUDFUtils.isScalarFunction(functionName)) {
        ScalarFunction scalarFunction = TableUDFUtils.getScalarFunction(functionName);
        List<ColumnTransformer> childrenColumnTransformer =
            children.stream().map(child -> process(child, context)).collect(Collectors.toList());
        FunctionParameters parameters =
            new FunctionParameters(
                childrenColumnTransformer.stream()
                    .map(i -> UDFDataTypeTransformer.transformReadTypeToUDFDataType(i.getType()))
                    .collect(Collectors.toList()),
                Collections.emptyMap());
        ScalarFunctionConfig config = new ScalarFunctionConfig();
        scalarFunction.beforeStart(parameters, config);
        Type returnType =
            UDFDataTypeTransformer.transformUDFDataTypeToReadType(config.getOutputDataType());
        return new UserDefineScalarFunctionTransformer(
            returnType, scalarFunction, childrenColumnTransformer);
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Unknown function %s on DataNode: %d.",
            functionName, IoTDBDescriptor.getInstance().getConfig().getDataNodeId()));
  }

  @Override
  protected ColumnTransformer visitInPredicate(InPredicate node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                BOOLEAN, context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(TSDataType.BOOLEAN);
        context.cache.put(node, identity);
      } else {
        ColumnTransformer childColumnTransformer = process(node.getValue(), context);
        TypeEnum childTypeEnum = childColumnTransformer.getType().getTypeEnum();
        InListExpression inListExpression = (InListExpression) node.getValueList();
        List<Expression> expressionList = inListExpression.getValues();
        List<Literal> values = new ArrayList<>();
        List<ColumnTransformer> valueColumnTransformerList = new ArrayList<>();
        valueColumnTransformerList.add(childColumnTransformer);
        for (Expression expression : expressionList) {
          if (expression instanceof Literal) {
            values.add((Literal) expression);
          } else {
            valueColumnTransformerList.add(process(expression, context));
          }
        }
        context.cache.put(
            node, constructInColumnTransformer(childTypeEnum, valueColumnTransformerList, values));
      }
    }

    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  private static InMultiColumnTransformer constructInColumnTransformer(
      TypeEnum childType,
      List<ColumnTransformer> valueColumnTransformerList,
      List<Literal> values) {
    String errorMsg = "\"%s\" cannot be cast to [%s]";
    switch (childType) {
      case INT32:
        Set<Integer> intSet = new HashSet<>();
        for (Literal value : values) {
          try {
            long v = ((LongLiteral) value).getParsedValue();
            if (v <= Integer.MAX_VALUE && v >= Integer.MIN_VALUE) {
              intSet.add((int) v);
            }
          } catch (IllegalArgumentException e) {
            throw new SemanticException(String.format(errorMsg, value, childType));
          }
        }
        return new InInt32MultiColumnTransformer(intSet, valueColumnTransformerList);
      case DATE:
        Set<Integer> dateSet = new HashSet<>();
        for (Literal value : values) {
          dateSet.add(Integer.parseInt(((GenericLiteral) value).getValue()));
        }
        return new InInt32MultiColumnTransformer(dateSet, valueColumnTransformerList);
      case INT64:
        Set<Long> longSet = new HashSet<>();
        for (Literal value : values) {
          longSet.add(((LongLiteral) value).getParsedValue());
        }
        return new InInt64MultiColumnTransformer(longSet, valueColumnTransformerList);
      case TIMESTAMP:
        Set<Long> timestampSet = new HashSet<>();
        for (Literal value : values) {
          try {
            if (value instanceof LongLiteral) {
              timestampSet.add(((LongLiteral) value).getParsedValue());
            } else if (value instanceof DoubleLiteral) {
              timestampSet.add((long) ((DoubleLiteral) value).getValue());
            } else if (value instanceof GenericLiteral) {
              timestampSet.add(Long.parseLong(((GenericLiteral) value).getValue()));
            } else {
              throw new SemanticException(
                  "InList Literal for TIMESTAMP can only be LongLiteral, DoubleLiteral and GenericLiteral, current is "
                      + value.getClass().getSimpleName());
            }
          } catch (IllegalArgumentException e) {
            throw new SemanticException(String.format(errorMsg, value, childType));
          }
        }
        return new InInt64MultiColumnTransformer(timestampSet, valueColumnTransformerList);
      case FLOAT:
        Set<Float> floatSet = new HashSet<>();
        for (Literal value : values) {
          try {
            floatSet.add((float) ((DoubleLiteral) value).getValue());
          } catch (IllegalArgumentException e) {
            throw new SemanticException(String.format(errorMsg, value, childType));
          }
        }
        return new InFloatMultiColumnTransformer(floatSet, valueColumnTransformerList);
      case DOUBLE:
        Set<Double> doubleSet = new HashSet<>();
        for (Literal value : values) {
          try {
            doubleSet.add(((DoubleLiteral) value).getValue());
          } catch (IllegalArgumentException e) {
            throw new SemanticException(String.format(errorMsg, value, childType));
          }
        }
        return new InDoubleMultiColumnTransformer(doubleSet, valueColumnTransformerList);
      case BOOLEAN:
        Set<Boolean> booleanSet = new HashSet<>();
        for (Literal value : values) {
          booleanSet.add(((BooleanLiteral) value).getValue());
        }
        return new InBooleanMultiColumnTransformer(booleanSet, valueColumnTransformerList);
      case TEXT:
      case STRING:
        Set<Binary> stringSet = new HashSet<>();
        for (Literal value : values) {
          stringSet.add(
              new Binary(((StringLiteral) value).getValue(), TSFileConfig.STRING_CHARSET));
        }
        return new InBinaryMultiColumnTransformer(stringSet, valueColumnTransformerList);
      case BLOB:
        Set<Binary> binarySet = new HashSet<>();
        for (Literal value : values) {
          binarySet.add(new Binary(((BinaryLiteral) value).getValue()));
        }
        return new InBinaryMultiColumnTransformer(binarySet, valueColumnTransformerList);
      default:
        throw new UnsupportedOperationException("unsupported data type: " + childType);
    }
  }

  @Override
  protected ColumnTransformer visitNotExpression(NotExpression node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                BOOLEAN, context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(TSDataType.BOOLEAN);
        context.cache.put(node, identity);
      } else {
        ColumnTransformer childColumnTransformer = process(node.getValue(), context);
        context.cache.put(node, new LogicNotColumnTransformer(BOOLEAN, childColumnTransformer));
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitLikePredicate(LikePredicate node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                BOOLEAN, context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(TSDataType.BOOLEAN);
        context.cache.put(node, identity);
      } else {
        ColumnTransformer likeColumnTransformer = null;
        ColumnTransformer childColumnTransformer = process(node.getValue(), context);
        if ((isStringLiteral(node.getPattern()) && !node.getEscape().isPresent())
            || (isStringLiteral(node.getPattern()) && isStringLiteral(node.getEscape().get()))) {
          Optional<Character> escapeSet =
              node.getEscape().isPresent()
                  ? getEscapeCharacter(((StringLiteral) node.getEscape().get()).getValue())
                  : Optional.empty();
          likeColumnTransformer =
              new LikeColumnTransformer(
                  BOOLEAN,
                  childColumnTransformer,
                  LikePattern.compile(((StringLiteral) node.getPattern()).getValue(), escapeSet));
        } else {
          ColumnTransformer patternColumnTransformer = process(node.getPattern(), context);
          if (node.getEscape().isPresent()) {
            ColumnTransformer escapeColumnTransformer = process(node.getEscape().get(), context);
            likeColumnTransformer =
                new Like3ColumnTransformer(
                    BOOLEAN,
                    childColumnTransformer,
                    patternColumnTransformer,
                    escapeColumnTransformer);
          } else {
            likeColumnTransformer =
                new Like2ColumnTransformer(
                    BOOLEAN, childColumnTransformer, patternColumnTransformer);
          }
        }

        context.cache.put(node, likeColumnTransformer);
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                BOOLEAN, context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(TSDataType.BOOLEAN);
        context.cache.put(node, identity);
      } else {
        ColumnTransformer childColumnTransformer = process(node.getValue(), context);
        context.cache.put(node, new IsNullColumnTransformer(BOOLEAN, childColumnTransformer, true));
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitIsNullPredicate(IsNullPredicate node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                BOOLEAN, context.originSize + context.commonTransformerList.size());
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(TSDataType.BOOLEAN);
        context.cache.put(node, identity);
      } else {
        ColumnTransformer childColumnTransformer = process(node.getValue(), context);
        context.cache.put(
            node, new IsNullColumnTransformer(BOOLEAN, childColumnTransformer, false));
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitLogicalExpression(LogicalExpression node, Context context) {
    ColumnTransformer logicalTransformer;
    if (context.cache.containsKey(node)) {
      logicalTransformer = context.cache.get(node);
    } else {
      logicalTransformer = getColumnTransformer(node, context);
      context.cache.put(node, logicalTransformer);
    }
    logicalTransformer.addReferenceCount();
    return logicalTransformer;
  }

  private ColumnTransformer getColumnTransformer(LogicalExpression node, Context context) {
    if (context.hasSeen.containsKey(node)) {
      IdentityColumnTransformer identity =
          new IdentityColumnTransformer(
              BOOLEAN, context.originSize + context.commonTransformerList.size());
      ColumnTransformer columnTransformer = context.hasSeen.get(node);
      columnTransformer.addReferenceCount();
      context.commonTransformerList.add(columnTransformer);
      context.leafList.add(identity);
      context.inputDataTypes.add(TSDataType.BOOLEAN);
      return identity;
    } else {
      List<ColumnTransformer> children =
          node.getChildren().stream().map(c -> process(c, context)).collect(Collectors.toList());
      switch (node.getOperator()) {
        case OR:
          return new LogicalOrMultiColumnTransformer(BOOLEAN, children);
        case AND:
          return new LogicalAndMultiColumnTransformer(BOOLEAN, children);
        default:
          throw new UnsupportedOperationException(
              String.format(UNSUPPORTED_EXPRESSION, node.getOperator()));
      }
    }
  }

  @Override
  protected ColumnTransformer visitSymbolReference(SymbolReference node, Context context) {
    ColumnTransformer res =
        context.cache.computeIfAbsent(
            node,
            e -> {
              int valueIdx =
                  context.inputLocations.get(Symbol.from(node)).get(0).getValueColumnIndex();
              LeafColumnTransformer leafColumnTransformer;
              if (valueIdx == -1) {
                leafColumnTransformer = new TimeColumnTransformer(INT64);
              } else {
                leafColumnTransformer =
                    new IdentityColumnTransformer(context.getType(node), valueIdx);
              }

              context.leafList.add(leafColumnTransformer);
              return leafColumnTransformer;
            });
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitCoalesceExpression(CoalesceExpression node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                columnTransformer.getType(),
                context.originSize + context.commonTransformerList.size());
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(getTSDataType(columnTransformer.getType()));
        context.cache.put(node, identity);
      } else {
        List<ColumnTransformer> children =
            node.getChildren().stream().map(c -> process(c, context)).collect(Collectors.toList());
        context.cache.put(node, new CoalesceColumnTransformer(children.get(0).getType(), children));
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitSimpleCaseExpression(
      SimpleCaseExpression node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                columnTransformer.getType(),
                context.originSize + context.commonTransformerList.size());
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(InternalTypeManager.getTSDataType(columnTransformer.getType()));
        context.cache.put(node, identity);
      } else {
        List<ColumnTransformer> whenList = new ArrayList<>();
        List<ColumnTransformer> thenList = new ArrayList<>();
        for (WhenClause whenClause : node.getWhenClauses()) {
          whenList.add(
              process(
                  new ComparisonExpression(
                      ComparisonExpression.Operator.EQUAL,
                      node.getOperand(),
                      whenClause.getOperand()),
                  context));
          thenList.add(process(whenClause.getResult(), context));
        }

        ColumnTransformer elseColumnTransformer =
            process(node.getDefaultValue().orElse(new NullLiteral()), context);
        context.cache.put(
            node,
            new TableCaseWhenThenColumnTransformer(
                thenList.get(0).getType(), whenList, thenList, elseColumnTransformer));
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitSearchedCaseExpression(
      SearchedCaseExpression node, Context context) {
    if (!context.cache.containsKey(node)) {
      if (context.hasSeen.containsKey(node)) {
        ColumnTransformer columnTransformer = context.hasSeen.get(node);
        IdentityColumnTransformer identity =
            new IdentityColumnTransformer(
                columnTransformer.getType(),
                context.originSize + context.commonTransformerList.size());
        columnTransformer.addReferenceCount();
        context.commonTransformerList.add(columnTransformer);
        context.leafList.add(identity);
        context.inputDataTypes.add(InternalTypeManager.getTSDataType(columnTransformer.getType()));
        context.cache.put(node, identity);
      } else {
        List<ColumnTransformer> whenList = new ArrayList<>();
        List<ColumnTransformer> thenList = new ArrayList<>();
        for (WhenClause whenClause : node.getWhenClauses()) {
          whenList.add(process(whenClause.getOperand(), context));
          thenList.add(process(whenClause.getResult(), context));
        }

        ColumnTransformer elseColumnTransformer =
            process(node.getDefaultValue().orElse(new NullLiteral()), context);

        context.cache.put(
            node,
            new TableCaseWhenThenColumnTransformer(
                thenList.get(0).getType(), whenList, thenList, elseColumnTransformer));
      }
    }
    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
  }

  @Override
  protected ColumnTransformer visitTrim(Trim node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitIfExpression(IfExpression node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitNullIfExpression(NullIfExpression node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  public static boolean isLongLiteral(Expression expression) {
    return expression instanceof LongLiteral;
  }

  public static boolean isBooleanLiteral(Expression expression) {
    return expression instanceof BooleanLiteral;
  }

  public static class Context {

    private final SessionInfo sessionInfo;

    // LeafColumnTransformer for LeafOperand
    private final List<LeafColumnTransformer> leafList;

    // Index of input column
    private final Map<Symbol, List<InputLocation>> inputLocations;

    // cache for constructing ColumnTransformer tree
    private final Map<Expression, ColumnTransformer> cache;

    // Sub expressions that has been seen in filter
    private final Map<Expression, ColumnTransformer> hasSeen;

    // Common Transformer between filter and project
    private final List<ColumnTransformer> commonTransformerList;

    private final List<TSDataType> inputDataTypes;

    private final int originSize;

    private final TypeProvider typeProvider;

    private final Metadata metadata;

    public Context(
        SessionInfo sessionInfo,
        List<LeafColumnTransformer> leafList,
        Map<Symbol, List<InputLocation>> inputLocations,
        Map<Expression, ColumnTransformer> cache,
        Map<Expression, ColumnTransformer> hasSeen,
        List<ColumnTransformer> commonTransformerList,
        List<TSDataType> inputDataTypes,
        int originSize,
        TypeProvider typeProvider,
        Metadata metadata) {
      this.sessionInfo = sessionInfo;
      this.leafList = leafList;
      this.inputLocations = inputLocations;
      this.cache = cache;
      this.hasSeen = hasSeen;
      this.commonTransformerList = commonTransformerList;
      this.inputDataTypes = inputDataTypes;
      this.originSize = originSize;
      this.typeProvider = typeProvider;
      this.metadata = metadata;
    }

    public Type getType(SymbolReference symbolReference) {
      return typeProvider.getTableModelType(Symbol.from(symbolReference));
    }
  }
}
