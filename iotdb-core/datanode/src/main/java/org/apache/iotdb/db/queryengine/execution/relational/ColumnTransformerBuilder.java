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

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.analyze.TypeProvider;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
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
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeNotFoundException;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticAdditionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticDivisionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticModuloColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticMultiplicationColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.ArithmeticSubtractionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareEqualToColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareGreaterEqualColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareGreaterThanColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareLessEqualColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareLessThanColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.CompareNonEqualColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.ConstantColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.NullColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.TimeColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.LogicalAndMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.LogicalOrMultiColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ternary.BetweenColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.ArithmeticNegationColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.InColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.IsNullColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.LogicNotColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.RegularColumnTransformer;
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
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.SubStringFunctionColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TableBuiltinScalarFunction;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TanColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TanhColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.Trim2ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.TrimColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar.UpperColumnTransformer;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BinaryColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.DoubleColumn;
import org.apache.tsfile.read.common.block.column.IntColumn;
import org.apache.tsfile.read.common.block.column.LongColumn;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.plan.relational.analyzer.predicate.PredicatePushIntoMetadataChecker.isStringLiteral;
import static org.apache.iotdb.db.queryengine.plan.relational.type.InternalTypeManager.getTSDataType;
import static org.apache.iotdb.db.queryengine.plan.relational.type.TypeSignatureTranslator.toTypeSignature;
import static org.apache.tsfile.read.common.type.BlobType.BLOB;
import static org.apache.tsfile.read.common.type.BooleanType.BOOLEAN;
import static org.apache.tsfile.read.common.type.DoubleType.DOUBLE;
import static org.apache.tsfile.read.common.type.IntType.INT32;
import static org.apache.tsfile.read.common.type.LongType.INT64;
import static org.apache.tsfile.read.common.type.StringType.STRING;
import static org.apache.tsfile.utils.RegexUtils.compileRegex;
import static org.apache.tsfile.utils.RegexUtils.parseLikePatternToRegex;

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
        ColumnTransformer left = process(node.getLeft(), context);
        ColumnTransformer right = process(node.getRight(), context);
        ColumnTransformer child;
        switch (node.getOperator()) {
          case ADD:
            child = new ArithmeticAdditionColumnTransformer(DOUBLE, left, right);
            break;
          case SUBTRACT:
            child = new ArithmeticSubtractionColumnTransformer(DOUBLE, left, right);
            break;
          case MULTIPLY:
            child = new ArithmeticMultiplicationColumnTransformer(DOUBLE, left, right);
            break;
          case DIVIDE:
            child = new ArithmeticDivisionColumnTransformer(DOUBLE, left, right);
            break;
          case MODULUS:
            child = new ArithmeticModuloColumnTransformer(DOUBLE, left, right);
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
                node, new ArithmeticNegationColumnTransformer(DOUBLE, childColumnTransformer));
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
            node, new CastFunctionColumnTransformer(type, child, context.sessionInfo.getZoneId()));
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
              first.getType(), first, ((StringLiteral) children.get(1)).getValue(), "");
        } else {
          return new Replace2ColumnTransformer(
              first.getType(), first, this.process(children.get(1), context));
        }
      } else {
        // size == 3
        if (isStringLiteral(children.get(1)) && isStringLiteral(children.get(2))) {
          return new ReplaceFunctionColumnTransformer(
              first.getType(),
              first,
              ((StringLiteral) children.get(1)).getValue(),
              ((StringLiteral) children.get(2)).getValue());
        } else {
          return new Replace3ColumnTransformer(
              first.getType(),
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
          if (startIndex <= 0) {
            throw new SemanticException(
                "Argument exception,the scalar function [SUBSTRING] beginPosition and length must be greater than 0");
          }
          return new SubStringFunctionColumnTransformer(
              first.getType(), first, startIndex, Integer.MAX_VALUE);
        } else {
          return new SubString2ColumnTransformer(
              first.getType(), first, this.process(children.get(1), context));
        }
      } else {
        // size == 3
        if (isLongLiteral(children.get(1)) && isLongLiteral(children.get(2))) {
          int startIndex = (int) ((LongLiteral) children.get(1)).getParsedValue();
          int length = (int) ((LongLiteral) children.get(2)).getParsedValue();
          if (startIndex <= 0 || length <= 0) {
            throw new SemanticException(
                "Argument exception,the scalar function [SUBSTRING] beginPosition and length must be greater than 0");
          }
          return new SubStringFunctionColumnTransformer(first.getType(), first, startIndex, length);
        } else {
          return new SubString3ColumnTransformer(
              first.getType(),
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
    }
    throw new IllegalArgumentException(String.format("Unknown function: %s", functionName));
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
        InListExpression inListExpression = (InListExpression) node.getValueList();
        List<Expression> expressionList = inListExpression.getValues();
        List<Literal> values = new ArrayList<>();
        for (Expression expression : expressionList) {
          checkArgument(expression instanceof Literal);
          values.add((Literal) expression);
        }
        context.cache.put(node, new InColumnTransformer(BOOLEAN, childColumnTransformer, values));
      }
    }

    ColumnTransformer res = context.cache.get(node);
    res.addReferenceCount();
    return res;
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
        ColumnTransformer childColumnTransformer = process(node.getValue(), context);
        context.cache.put(
            node,
            new RegularColumnTransformer(
                BOOLEAN,
                childColumnTransformer,
                compileRegex(
                    parseLikePatternToRegex(((StringLiteral) node.getPattern()).getValue()))));
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
  protected ColumnTransformer visitSimpleCaseExpression(
      SimpleCaseExpression node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
  }

  @Override
  protected ColumnTransformer visitSearchedCaseExpression(
      SearchedCaseExpression node, Context context) {
    throw new UnsupportedOperationException(String.format(UNSUPPORTED_EXPRESSION, node));
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

  @Override
  protected ColumnTransformer visitCoalesceExpression(CoalesceExpression node, Context context) {
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
