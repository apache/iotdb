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

package org.apache.iotdb.commons.queryengine.plan.relational.sql.util;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AllColumns;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.AllRows;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ArithmeticBinaryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ArithmeticUnaryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BetweenPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BinaryLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Cast;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CoalesceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Columns;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CommonQueryAstVisitor;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CurrentDatabase;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CurrentTime;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.CurrentUser;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DecimalLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DereferenceExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.DoubleLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ExistsPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Extract;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FieldReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FloatLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FrameBound;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.FunctionCall;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericDataType;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GenericLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GroupingElement;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.GroupingSets;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IfExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InListExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.InPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LikePredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Literal;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Node;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NotExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullIfExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NullLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.NumericParameter;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.OrderBy;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Parameter;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QualifiedName;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.QuantifiedComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Row;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SearchedCaseExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SimpleCaseExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SimpleGroupBy;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SortItem;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SubqueryExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.SymbolReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TimeDurationLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Trim;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.TypeParameter;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WhenClause;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Window;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WindowFrame;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WindowReference;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.WindowSpecification;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.util.CommonQuerySqlFormatter.formatName;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.util.ReservedIdentifiers.reserved;
import static org.apache.iotdb.commons.udf.builtin.relational.TableBuiltinScalarFunction.DATE_BIN;

public final class ExpressionFormatter {

  private static final ThreadLocal<DecimalFormat> doubleFormatter =
      ThreadLocal.withInitial(
          () ->
              new DecimalFormat("0.###################E0###", new DecimalFormatSymbols(Locale.US)));

  private ExpressionFormatter() {}

  public static String formatExpression(Expression expression) {
    return new Formatter(Optional.empty(), Optional.empty()).process(expression, null);
  }

  private static String formatSql(Node root) {
    StringBuilder builder = new StringBuilder();
    new CommonQuerySqlFormatter(builder).process(root, 0);
    return builder.toString();
  }

  public static class Formatter implements CommonQueryAstVisitor<String, Void> {
    private final Optional<Function<Literal, String>> literalFormatter;
    private final Optional<Function<SymbolReference, String>> symbolReferenceFormatter;

    public Formatter(
        Optional<Function<Literal, String>> literalFormatter,
        Optional<Function<SymbolReference, String>> symbolReferenceFormatter) {
      this.literalFormatter = requireNonNull(literalFormatter, "literalFormatter is null");
      this.symbolReferenceFormatter =
          requireNonNull(symbolReferenceFormatter, "symbolReferenceFormatter is null");
    }

    @Override
    public String visitNode(Node node, Void context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String visitRow(Row node, Void context) {
      return node.getItems().stream()
          .map(child -> process(child, context))
          .collect(joining(", ", "ROW (", ")"));
    }

    @Override
    public String visitExpression(Expression node, Void context) {
      throw new UnsupportedOperationException(
          String.format(
              "not yet implemented: %s.visit%s",
              getClass().getName(), node.getClass().getSimpleName()));
    }

    @Override
    public String visitCurrentDatabase(CurrentDatabase node, Void context) {
      return "CURRENT_DATABASE";
    }

    @Override
    public String visitCurrentUser(CurrentUser node, Void context) {
      return "CURRENT_USER";
    }

    @Override
    public String visitTrim(Trim node, Void context) {
      if (!node.getTrimCharacter().isPresent()) {
        return String.format(
            "trim(%s FROM %s)", node.getSpecification(), process(node.getTrimSource(), context));
      }

      return String.format(
          "trim(%s %s FROM %s)",
          node.getSpecification(),
          process(node.getTrimCharacter().get(), context),
          process(node.getTrimSource(), context));
    }

    @Override
    public String visitCurrentTime(CurrentTime node, Void context) {
      StringBuilder builder = new StringBuilder();

      builder.append(node.getFunction().getName());

      if (node.getPrecision().isPresent()) {
        builder.append('(').append(node.getPrecision()).append(')');
      }

      return builder.toString();
    }

    @Override
    public String visitExtract(Extract node, Void context) {
      return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), context) + ")";
    }

    @Override
    public String visitBooleanLiteral(BooleanLiteral node, Void context) {
      return literalFormatter
          .map(formatter -> formatter.apply(node))
          .orElseGet(() -> String.valueOf(node.getValue()));
    }

    @Override
    public String visitStringLiteral(StringLiteral node, Void context) {
      return literalFormatter
          .map(formatter -> formatter.apply(node))
          .orElseGet(() -> formatStringLiteral(node.getValue()));
    }

    @Override
    public String visitBinaryLiteral(BinaryLiteral node, Void context) {
      return literalFormatter
          .map(formatter -> formatter.apply(node))
          .orElseGet(() -> "X'" + node.toHexString() + "'");
    }

    @Override
    public String visitParameter(Parameter node, Void context) {
      return "?";
    }

    @Override
    public String visitAllRows(AllRows node, Void context) {
      return "ALL";
    }

    @Override
    public String visitLongLiteral(LongLiteral node, Void context) {
      return literalFormatter.map(formatter -> formatter.apply(node)).orElseGet(node::getValue);
    }

    @Override
    public String visitDoubleLiteral(DoubleLiteral node, Void context) {
      return literalFormatter
          .map(formatter -> formatter.apply(node))
          .orElseGet(() -> doubleFormatter.get().format(node.getValue()));
    }

    // do not use doubleFormatter, to prevent from introducing the precision noise
    @Override
    public String visitFloatLiteral(FloatLiteral node, Void context) {
      return literalFormatter
          .map(formatter -> formatter.apply(node))
          .orElseGet(() -> String.valueOf(node.getValue()));
    }

    @Override
    public String visitDecimalLiteral(DecimalLiteral node, Void context) {
      return literalFormatter
          .map(formatter -> formatter.apply(node))
          // TODO return node value without "DECIMAL '..'" when
          // FeaturesConfig#parseDecimalLiteralsAsDouble switch is removed
          .orElseGet(() -> "DECIMAL '" + node.getValue() + "'");
    }

    @Override
    public String visitGenericLiteral(GenericLiteral node, Void context) {
      return literalFormatter
          .map(formatter -> formatter.apply(node))
          .orElseGet(() -> node.getType() + " " + formatStringLiteral(node.getValue()));
    }

    @Override
    public String visitNullLiteral(NullLiteral node, Void context) {
      return literalFormatter.map(formatter -> formatter.apply(node)).orElse("null");
    }

    @Override
    public String visitTimeDurationLiteral(TimeDurationLiteral node, Void context) {
      return node.getValue().toString();
    }

    @Override
    public String visitSubqueryExpression(SubqueryExpression node, Void context) {
      return "(" + formatSql(node.getQuery()) + ")";
    }

    @Override
    public String visitExists(ExistsPredicate node, Void context) {
      return "(EXISTS " + formatSql(node.getSubquery()) + ")";
    }

    @Override
    public String visitIdentifier(Identifier node, Void context) {
      if (node.isDelimited() || reserved(node.getValue())) {
        return '"' + node.getValue().replace("\"", "\"\"") + '"';
      }
      return node.getValue();
    }

    @Override
    public String visitSymbolReference(SymbolReference node, Void context) {
      if (symbolReferenceFormatter.isPresent()) {
        return symbolReferenceFormatter.get().apply(node);
      }
      return formatIdentifier(node.getName());
    }

    private String formatIdentifier(String s) {
      return '"' + s.replace("\"", "\"\"") + '"';
    }

    @Override
    public String visitDereferenceExpression(DereferenceExpression node, Void context) {
      String baseString = process(node.getBase(), context);
      return baseString + "." + node.getField().map(this::process).orElse("*");
    }

    @Override
    public String visitFieldReference(FieldReference node, Void context) {
      // add colon so this won't parse
      return ":input(" + node.getFieldIndex() + ")";
    }

    @Override
    public String visitFunctionCall(FunctionCall node, Void context) {
      if (QualifiedName.of("LISTAGG").equals(node.getName())) {
        return visitListagg(node);
      }

      StringBuilder builder = new StringBuilder();

      if (node.getProcessingMode().isPresent()) {
        builder.append(node.getProcessingMode().get().getMode()).append(" ");
      }

      String arguments = joinExpressions(node.getArguments());
      if (node.getArguments().isEmpty() && "count".equalsIgnoreCase(node.getName().getSuffix())) {
        arguments = "*";
      }
      if (node.isDistinct()) {
        arguments = "DISTINCT " + arguments;
      }
      // deal with date_bin_gapfill
      if (QualifiedName.of(DATE_BIN.getFunctionName()).equals(node.getName())
          && node.getArguments().size() == 5) {
        arguments = joinExpressions(node.getArguments().subList(0, 5));
        builder
            .append(formatName(QualifiedName.of(DATE_BIN.getFunctionName() + "_gapfill")))
            .append('(')
            .append(arguments);
      } else {
        builder.append(formatName(node.getName())).append('(').append(arguments);
      }

      builder.append(')');

      if (node.getWindow().isPresent()) {
        builder.append(" OVER ").append(formatWindow(node.getWindow().get()));
      }

      return builder.toString();
    }

    @Override
    public String visitLogicalExpression(LogicalExpression node, Void context) {
      return "("
          + node.getTerms().stream()
              .map(term -> process(term, context))
              .collect(joining(" " + node.getOperator().toString() + " "))
          + ")";
    }

    @Override
    public String visitNotExpression(NotExpression node, Void context) {
      return "(NOT " + process(node.getValue(), context) + ")";
    }

    @Override
    public String visitComparisonExpression(ComparisonExpression node, Void context) {
      return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
    }

    @Override
    public String visitIsNullPredicate(IsNullPredicate node, Void context) {
      return "(" + process(node.getValue(), context) + " IS NULL)";
    }

    @Override
    public String visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
      return "(" + process(node.getValue(), context) + " IS NOT NULL)";
    }

    @Override
    public String visitNullIfExpression(NullIfExpression node, Void context) {
      return "NULLIF("
          + process(node.getFirst(), context)
          + ", "
          + process(node.getSecond(), context)
          + ')';
    }

    @Override
    public String visitIfExpression(IfExpression node, Void context) {
      StringBuilder builder = new StringBuilder();
      builder
          .append("IF(")
          .append(process(node.getCondition(), context))
          .append(", ")
          .append(process(node.getTrueValue(), context));
      node.getFalseValue()
          .map(expression -> builder.append(", ").append(process(expression, context)));
      builder.append(")");
      return builder.toString();
    }

    @Override
    public String visitCoalesceExpression(CoalesceExpression node, Void context) {
      return "COALESCE(" + joinExpressions(node.getOperands()) + ")";
    }

    @Override
    public String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
      String value = process(node.getValue(), context);

      switch (node.getSign()) {
        // Unary is ambiguous with respect to negative numbers. "-1" parses as a number, but
        // "-(1)" parses as "unaryMinus(number)"
        // The parentheses are needed to ensure the parsing roundtrips properly.
        case MINUS:
          return "-(" + value + ")";
        case PLUS:
          return "+" + value;
        default:
          throw new IllegalArgumentException("Unknown sign: " + node.getSign());
      }
    }

    @Override
    public String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
      return formatBinaryExpression(node.getOperator().getValue(), node.getLeft(), node.getRight());
    }

    @Override
    public String visitLikePredicate(LikePredicate node, Void context) {
      StringBuilder builder = new StringBuilder();

      builder
          .append('(')
          .append(process(node.getValue(), context))
          .append(" LIKE ")
          .append(process(node.getPattern(), context));

      node.getEscape()
          .ifPresent(escape -> builder.append(" ESCAPE ").append(process(escape, context)));

      builder.append(')');

      return builder.toString();
    }

    @Override
    public String visitAllColumns(AllColumns node, Void context) {
      StringBuilder builder = new StringBuilder();
      if (node.getTarget().isPresent()) {
        builder.append(process(node.getTarget().get(), context));
        builder.append(".*");
      } else {
        builder.append("*");
      }

      if (!node.getAliases().isEmpty()) {
        builder.append(" AS (");
        Joiner.on(", ")
            .appendTo(
                builder,
                node.getAliases().stream().map(alias -> process(alias, context)).collect(toList()));
        builder.append(")");
      }

      return builder.toString();
    }

    @Override
    public String visitCast(Cast node, Void context) {
      return (node.isSafe() ? "TRY_CAST" : "CAST")
          + "("
          + process(node.getExpression(), context)
          + " AS "
          + process(node.getType(), context)
          + ")";
    }

    @Override
    public String visitSearchedCaseExpression(SearchedCaseExpression node, Void context) {
      ImmutableList.Builder<String> parts = ImmutableList.builder();
      parts.add("CASE");
      for (WhenClause whenClause : node.getWhenClauses()) {
        parts.add(process(whenClause, context));
      }

      node.getDefaultValue().ifPresent(value -> parts.add("ELSE").add(process(value, context)));

      parts.add("END");

      return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
      ImmutableList.Builder<String> parts = ImmutableList.builder();

      parts.add("CASE").add(process(node.getOperand(), context));

      for (WhenClause whenClause : node.getWhenClauses()) {
        parts.add(process(whenClause, context));
      }

      node.getDefaultValue().ifPresent(value -> parts.add("ELSE").add(process(value, context)));

      parts.add("END");

      return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String visitWhenClause(WhenClause node, Void context) {
      return "WHEN "
          + process(node.getOperand(), context)
          + " THEN "
          + process(node.getResult(), context);
    }

    @Override
    public String visitBetweenPredicate(BetweenPredicate node, Void context) {
      return "("
          + process(node.getValue(), context)
          + " BETWEEN "
          + process(node.getMin(), context)
          + " AND "
          + process(node.getMax(), context)
          + ")";
    }

    @Override
    public String visitInPredicate(InPredicate node, Void context) {
      return "("
          + process(node.getValue(), context)
          + " IN "
          + process(node.getValueList(), context)
          + ")";
    }

    @Override
    public String visitInListExpression(InListExpression node, Void context) {
      return "(" + joinExpressions(node.getValues()) + ")";
    }

    @Override
    public String visitQuantifiedComparisonExpression(
        QuantifiedComparisonExpression node, Void context) {
      return String.format(
          "(%s %s %s %s)",
          process(node.getValue(), context),
          node.getOperator().getValue(),
          node.getQuantifier(),
          process(node.getSubquery(), context));
    }

    @Override
    public String visitGenericDataType(GenericDataType node, Void context) {
      StringBuilder result = new StringBuilder();
      result.append(node.getName());

      if (!node.getArguments().isEmpty()) {
        result.append(
            node.getArguments().stream().map(this::process).collect(joining(", ", "(", ")")));
      }

      return result.toString();
    }

    @Override
    public String visitTypeParameter(TypeParameter node, Void context) {
      return process(node.getValue(), context);
    }

    @Override
    public String visitNumericTypeParameter(NumericParameter node, Void context) {
      return node.getValue();
    }

    @Override
    public String visitColumns(Columns node, Void context) {
      return "COLUMNS(" + (node.isColumnsAsterisk() ? "*" : node.getPattern()) + ")";
    }

    private String formatBinaryExpression(String operator, Expression left, Expression right) {
      return '(' + process(left, null) + ' ' + operator + ' ' + process(right, null) + ')';
    }

    private String joinExpressions(List<Expression> expressions) {
      return expressions.stream().map(e -> process(e, null)).collect(joining(", "));
    }

    /**
     * Returns the formatted `LISTAGG` function call corresponding to the specified node.
     *
     * <p>During the parsing of the syntax tree, the `LISTAGG` expression is synthetically converted
     * to a function call. This method formats the specified {@link FunctionCall} node to correspond
     * to the standardised syntax of the `LISTAGG` expression.
     *
     * @param node the `LISTAGG` function call
     */
    private String visitListagg(FunctionCall node) {
      StringBuilder builder = new StringBuilder();

      List<Expression> arguments = node.getArguments();
      Expression expression = arguments.get(0);
      Expression separator = arguments.get(1);
      BooleanLiteral overflowError = (BooleanLiteral) arguments.get(2);
      Expression overflowFiller = arguments.get(3);
      BooleanLiteral showOverflowEntryCount = (BooleanLiteral) arguments.get(4);

      String innerArguments = joinExpressions(ImmutableList.of(expression, separator));
      if (node.isDistinct()) {
        innerArguments = "DISTINCT " + innerArguments;
      }

      builder.append("LISTAGG").append('(').append(innerArguments);

      builder.append(" ON OVERFLOW ");
      if (overflowError.getValue()) {
        builder.append(" ERROR");
      } else {
        builder.append(" TRUNCATE").append(' ').append(process(overflowFiller, null));
        if (showOverflowEntryCount.getValue()) {
          builder.append(" WITH COUNT");
        } else {
          builder.append(" WITHOUT COUNT");
        }
      }

      builder.append(')');

      return builder.toString();
    }
  }

  static String formatStringLiteral(String s) {
    return "'" + s.replace("'", "''") + "'";
  }

  public static String formatOrderBy(OrderBy orderBy) {
    return "ORDER BY " + formatSortItems(orderBy.getSortItems());
  }

  public static String formatSortItems(List<SortItem> sortItems) {
    return sortItems.stream().map(sortItemFormatterFunction()).collect(joining(", "));
  }

  private static String formatWindow(Window window) {
    if (window instanceof WindowReference) {
      return formatExpression(((WindowReference) window).getName());
    }

    return formatWindowSpecification((WindowSpecification) window);
  }

  static String formatWindowSpecification(WindowSpecification windowSpecification) {
    List<String> parts = new ArrayList<>();

    if (windowSpecification.getExistingWindowName().isPresent()) {
      parts.add(formatExpression(windowSpecification.getExistingWindowName().get()));
    }
    if (!windowSpecification.getPartitionBy().isEmpty()) {
      parts.add(
          "PARTITION BY "
              + windowSpecification.getPartitionBy().stream()
                  .map(ExpressionFormatter::formatExpression)
                  .collect(joining(", ")));
    }
    if (windowSpecification.getOrderBy().isPresent()) {
      parts.add(formatOrderBy(windowSpecification.getOrderBy().get()));
    }
    if (windowSpecification.getFrame().isPresent()) {
      parts.add(formatFrame(windowSpecification.getFrame().get()));
    }

    return '(' + Joiner.on(' ').join(parts) + ')';
  }

  private static String formatFrame(WindowFrame windowFrame) {
    StringBuilder builder = new StringBuilder();

    builder.append(windowFrame.getType().toString()).append(' ');

    if (windowFrame.getEnd().isPresent()) {
      builder
          .append("BETWEEN ")
          .append(formatFrameBound(windowFrame.getStart()))
          .append(" AND ")
          .append(formatFrameBound(windowFrame.getEnd().get()));
    } else {
      builder.append(formatFrameBound(windowFrame.getStart()));
    }

    return builder.toString();
  }

  private static String formatFrameBound(FrameBound frameBound) {
    switch (frameBound.getType()) {
      case UNBOUNDED_PRECEDING:
        return "UNBOUNDED PRECEDING";
      case PRECEDING:
        return formatExpression(frameBound.getValue().get()) + " PRECEDING";
      case CURRENT_ROW:
        return "CURRENT ROW";
      case FOLLOWING:
        return formatExpression(frameBound.getValue().get()) + " FOLLOWING";
      case UNBOUNDED_FOLLOWING:
        return "UNBOUNDED FOLLOWING";
      default:
        throw new IllegalArgumentException("Unsupported frame type: " + frameBound.getType());
    }
  }

  public static String formatGroupBy(List<GroupingElement> groupingElements) {
    return groupingElements.stream()
        .map(
            groupingElement -> {
              String result = "";
              if (groupingElement instanceof SimpleGroupBy) {
                List<Expression> columns = groupingElement.getExpressions();
                if (columns.size() == 1) {
                  result = formatExpression(getOnlyElement(columns));
                } else {
                  result = formatGroupingSet(columns);
                }
              } else if (groupingElement instanceof GroupingSets) {
                GroupingSets groupingSets = (GroupingSets) groupingElement;
                String type = null;
                switch (groupingSets.getType()) {
                  case EXPLICIT:
                    type = "GROUPING SETS";
                    break;
                  case CUBE:
                    type = "CUBE";
                    break;
                  case ROLLUP:
                    type = "ROLLUP";
                    break;
                }

                result =
                    groupingSets.getSets().stream()
                        .map(ExpressionFormatter::formatGroupingSet)
                        .collect(joining(", ", type + " (", ")"));
              }
              return result;
            })
        .collect(joining(", "));
  }

  private static boolean isAsciiPrintable(int codePoint) {
    return codePoint >= 0x20 && codePoint < 0x7F;
  }

  private static String formatGroupingSet(List<Expression> groupingSet) {
    return groupingSet.stream()
        .map(ExpressionFormatter::formatExpression)
        .collect(joining(", ", "(", ")"));
  }

  private static Function<SortItem, String> sortItemFormatterFunction() {
    return input -> {
      StringBuilder builder = new StringBuilder();

      builder.append(formatExpression(input.getSortKey()));

      switch (input.getOrdering()) {
        case ASCENDING:
          builder.append(" ASC");
          break;
        case DESCENDING:
          builder.append(" DESC");
          break;
      }

      switch (input.getNullOrdering()) {
        case FIRST:
          builder.append(" NULLS FIRST");
          break;
        case LAST:
          builder.append(" NULLS LAST");
          break;
      }

      return builder.toString();
    };
  }
}
