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
package org.apache.iotdb.db.tools;

import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Expression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Identifier;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNotNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.IsNullPredicate;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LongLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.StringLiteral;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.util.ExpressionFormatter;
import org.apache.iotdb.db.i18n.ImportWALMessages;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate;
import org.apache.iotdb.rpc.StatementExecutionException;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral.FALSE_LITERAL;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.BooleanLiteral.TRUE_LITERAL;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.EQUAL;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression.Operator.AND;
import static org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.LogicalExpression.Operator.OR;

/**
 * Reconstructs row deletes using the target table's ordered TAG columns. The WAL stores TAG
 * ordinals, not names, so the target TAG order must match the source table.
 */
final class TableWALDeleteConverter {

  private final String tableName;
  private final String timeColumn;
  private final List<String> tagColumns;

  TableWALDeleteConverter(
      final String tableName, final String timeColumn, final List<String> tagColumns) {
    this.tableName = tableName;
    this.timeColumn = timeColumn;
    this.tagColumns = tagColumns;
  }

  /** Returns null for a deletion that cannot match any row. */
  String toSql(final List<TableDeletionEntry> entries) throws ConversionException {
    if (timeColumn == null) {
      throw new ConversionException(
          String.format(
              ImportWALMessages
                  .EXCEPTION_CANNOT_REPLAY_TABLE_DELETION_TARGET_TABLE_ARG_HAS_NO_TIME_COLUMN_55B1C25C,
              tableName));
    }
    final List<Expression> alternatives = new ArrayList<>();
    for (final TableDeletionEntry entry : entries) {
      // DELETE FROM removes whole rows; translating a column tombstone would widen the deletion.
      if (!entry.getPredicate().getMeasurementNames().isEmpty()) {
        throw new ConversionException(
            String.format(
                ImportWALMessages
                    .EXCEPTION_CANNOT_REPLAY_COLUMN_SPECIFIC_DELETION_FOR_TABLE_ARG_AS_DELETE_FROM_4A7ACC93,
                tableName));
      }
      final List<Expression> conditions = new ArrayList<>();
      conditions.add(toExpression(entry.getPredicate().getTagPredicate()));
      // Omit unbounded ends instead of doing arithmetic on inclusive long boundaries.
      if (entry.getStartTime() != Long.MIN_VALUE) {
        conditions.add(timeComparison(GREATER_THAN_OR_EQUAL, entry.getStartTime()));
      }
      if (entry.getEndTime() != Long.MAX_VALUE) {
        conditions.add(timeComparison(LESS_THAN_OR_EQUAL, entry.getEndTime()));
      }
      alternatives.add(combine(AND, conditions));
    }
    // Keep each TAG predicate paired with its own interval when merging deletes for one table.
    final Expression predicate = combine(OR, alternatives);
    if (predicate == FALSE_LITERAL) {
      return null;
    }
    return "DELETE FROM "
        + ImportWAL.WALReplayer.quoteIdentifier(tableName)
        + (predicate == TRUE_LITERAL
            ? ""
            : " WHERE " + ExpressionFormatter.formatExpression(predicate));
  }

  private Expression timeComparison(final ComparisonExpression.Operator operator, final long time) {
    return new ComparisonExpression(
        operator, new Identifier(timeColumn, true), new LongLiteral(Long.toString(time)));
  }

  private Expression toExpression(final TagPredicate predicate) throws ConversionException {
    if (predicate instanceof TagPredicate.NOP) {
      return TRUE_LITERAL;
    }
    if (predicate instanceof TagPredicate.SegmentExactMatch match) {
      validateSegmentIndex(match.getSegmentIndex());
      return match.getSegmentIndex() == 0
          ? (Objects.equals(tableName, match.getPattern()) ? TRUE_LITERAL : FALSE_LITERAL)
          : tagComparison(match.getSegmentIndex(), match.getPattern());
    }
    if (predicate instanceof TagPredicate.SegmentNotNull notNull) {
      validateSegmentIndex(notNull.getSegmentIndex());
      return notNull.getSegmentIndex() == 0
          ? TRUE_LITERAL
          : new IsNotNullPredicate(tagIdentifier(notNull.getSegmentIndex()));
    }
    if (predicate instanceof TagPredicate.FullExactMatch match) {
      return deviceExpression(match.getDeviceID());
    }
    if (predicate instanceof TagPredicate.DeviceIn deviceIn) {
      final List<Expression> devices = new ArrayList<>();
      // Attribute-based deletes store the resolved device set in the WAL. Replaying that set
      // avoids re-evaluating attributes that may have changed since the original deletion.
      for (final IDeviceID device : deviceIn.getDeviceIDs()) {
        devices.add(deviceExpression(device));
      }
      return combine(OR, devices);
    }
    if (predicate instanceof TagPredicate.And and) {
      final List<Expression> conditions = new ArrayList<>();
      for (final TagPredicate child : and.getPredicates()) {
        conditions.add(toExpression(child));
      }
      return combine(AND, conditions);
    }
    throw new ConversionException(
        String.format(
            ImportWALMessages
                .EXCEPTION_CANNOT_REPLAY_TABLE_DELETION_UNSUPPORTED_TAG_PREDICATE_ARG_AD0753A8,
            predicate.getClass().getSimpleName()));
  }

  private Expression deviceExpression(final IDeviceID device) throws ConversionException {
    if (!tableName.equals(device.getTableName()) || device.segmentNum() > tagColumns.size() + 1) {
      throw new ConversionException(
          String.format(
              ImportWALMessages
                  .EXCEPTION_CANNOT_REPLAY_TABLE_DELETION_DEVICE_ARG_IS_INCOMPATIBLE_WITH_TARGET_TABLE_ARG_A6320535,
              device,
              tableName));
    }
    final List<Expression> conditions = new ArrayList<>();
    // Device IDs omit trailing null TAGs. Exact matching must explicitly constrain those TAGs.
    for (int index = 1; index <= tagColumns.size(); index++) {
      conditions.add(
          tagComparison(
              index, index < device.segmentNum() ? (String) device.segment(index) : null));
    }
    return combine(AND, conditions);
  }

  private Expression tagComparison(final int index, final String value) {
    final Identifier column = tagIdentifier(index);
    return value == null
        ? new IsNullPredicate(column)
        : new ComparisonExpression(EQUAL, column, new StringLiteral(value));
  }

  private Identifier tagIdentifier(final int index) {
    // Segment zero is the table name; TAG segments start at one in the WAL format.
    return new Identifier(tagColumns.get(index - 1), true);
  }

  private void validateSegmentIndex(final int index) throws ConversionException {
    if (index < 0 || index > tagColumns.size()) {
      throw new ConversionException(
          String.format(
              ImportWALMessages
                  .EXCEPTION_CANNOT_REPLAY_TABLE_DELETION_TAG_SEGMENT_INDEX_ARG_IS_INCOMPATIBLE_WITH_TARGET_TABLE_ARG_D5E3CCEE,
              index,
              tableName));
    }
  }

  private static Expression combine(
      final LogicalExpression.Operator operator, final List<Expression> expressions) {
    // DELETE's analyzer accepts TAG/time comparisons, not BooleanLiteral predicates. Use boolean
    // sentinels only internally and simplify them before generating any SQL.
    final Expression identity = operator == AND ? TRUE_LITERAL : FALSE_LITERAL;
    final Expression absorbing = operator == AND ? FALSE_LITERAL : TRUE_LITERAL;
    final List<Expression> terms = new ArrayList<>();
    for (final Expression expression : expressions) {
      if (expression == absorbing) {
        return absorbing;
      }
      if (expression != identity) {
        terms.add(expression);
      }
    }
    return switch (terms.size()) {
      case 0 -> identity;
      case 1 -> terms.get(0);
      default -> new LogicalExpression(operator, terms);
    };
  }

  /** Only conversion limitations may be skipped; session and execution failures remain fatal. */
  static final class ConversionException extends StatementExecutionException {

    private ConversionException(final String message) {
      super(message);
    }
  }
}
